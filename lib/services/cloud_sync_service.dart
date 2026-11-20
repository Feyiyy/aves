import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:aves/model/entry/entry.dart';
import 'package:aves/model/entry/origins.dart';
import 'package:aves/model/settings/settings.dart';
import 'package:aves/model/source/collection_source.dart';
import 'package:aves/services/common/services.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/painting.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;
import 'package:sqflite/sqflite.dart';

class CloudSyncProgress {
  final int downloadedBytes;
  final int totalBytes;
  final int extractedCount;
  final int totalCount;
  final double progress;
  final bool isSyncing;
  final bool isCompleted;
  final bool isPreempted;
  final int rateLimitBytesPerSec;
  final List<String> recentHashes;

  const CloudSyncProgress({
    this.downloadedBytes = 0,
    this.totalBytes = 0,
    this.extractedCount = 0,
    this.totalCount = 0,
    this.progress = 0.0,
    this.isSyncing = false,
    this.isCompleted = false,
    this.isPreempted = false,
    this.rateLimitBytesPerSec = 524288,
    this.recentHashes = const [],
  });

  factory CloudSyncProgress.fromMap(Map<dynamic, dynamic> map) {
    final rawHashes = map['recentHashes'];
    final hashes = rawHashes is List
        ? rawHashes.map((e) => e.toString().toLowerCase()).toList(growable: false)
        : const <String>[];

    return CloudSyncProgress(
      downloadedBytes: (map['downloadedBytes'] as num?)?.toInt() ?? 0,
      totalBytes: (map['totalBytes'] as num?)?.toInt() ?? 0,
      extractedCount: (map['extractedCount'] as num?)?.toInt() ?? 0,
      totalCount: (map['totalCount'] as num?)?.toInt() ?? 0,
      progress: ((map['progress'] as num?)?.toDouble() ?? 0.0).clamp(0.0, 1.0),
      isSyncing: map['isSyncing'] as bool? ?? false,
      isCompleted: map['isCompleted'] as bool? ?? false,
      isPreempted: map['isPreempted'] as bool? ?? false,
      rateLimitBytesPerSec: (map['rateLimitBytesPerSec'] as num?)?.toInt() ?? 524288,
      recentHashes: hashes,
    );
  }

  String get formattedSizePair => '${_formatBytes(downloadedBytes)} / ${_formatBytes(totalBytes)}';

  String get percentageLabel => '${(progress * 100).clamp(0, 100).toStringAsFixed(1)}%';

  static String _formatBytes(int bytes) {
    if (bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    var value = bytes.toDouble();
    var idx = 0;
    while (value >= 1024 && idx < units.length - 1) {
      value /= 1024;
      idx++;
    }
    return idx == 0 ? '${value.toInt()} B' : '${value.toStringAsFixed(1)} ${units[idx]}';
  }
}

final CloudSyncService cloudSyncService = CloudSyncService.instance;

class CloudSyncService {
  static final CloudSyncService instance = CloudSyncService._();

  static const MethodChannel _safChannel = MethodChannel('deckers.thibault/aves/cloud_saf');
  static const EventChannel _eventChannel = EventChannel('deckers.thibault/aves/cloud_sync_events');

  final ValueNotifier<CloudSyncProgress> progressNotifier =
      ValueNotifier<CloudSyncProgress>(const CloudSyncProgress());

  final Map<String, ValueNotifier<int>> _hashRevisions = <String, ValueNotifier<int>>{};
  final Map<String, Object> _cacheKeysByHash = <String, Object>{};
  final Map<String, AvesEntry> _entryBySha256 = <String, AvesEntry>{};

  StreamSubscription<dynamic>? _eventSubscription;
  bool _isSyncing = false;
  bool _initialized = false;
  bool _isPurgingDeletedEntries = false;
  bool _isAutoBackingUp = false;

  CloudSyncService._();

  void dispose() {
    _eventSubscription?.cancel();
    _eventSubscription = null;
    _initialized = false;
  }

  void ensureInitialized() {
    if (_initialized) return;
    _initialized = true;
    _eventSubscription = _eventChannel.receiveBroadcastStream().listen(
      _onEvent,
      onError: (Object error, StackTrace stackTrace) {
        debugPrint('CloudSyncService event stream error: $error');
      },
    );
    refreshStatus();
  }

  void _onEvent(dynamic event) {
    if (event is! Map) return;

    final type = event['type'] as String?;
    if (type == 'original_ready') {
      final sha256 = (event['sha256'] as String?)?.toLowerCase();
      if (sha256 != null && sha256.length >= 16) {
        _onOriginalReady(sha256);
      }
      return;
    }

    final next = CloudSyncProgress.fromMap(event);
    progressNotifier.value = next;

    if (next.recentHashes.isNotEmpty) {
      _evictAndBumpHashes(next.recentHashes);
    }
  }

  void _onOriginalReady(String sha256) {
    final hash = sha256.toLowerCase();
    PaintingBinding.instance.imageCache.clear();
    PaintingBinding.instance.imageCache.clearLiveImages();

    final boundKey = _cacheKeysByHash[hash];
    if (boundKey != null) {
      PaintingBinding.instance.imageCache.evict(boundKey, includeLive: true);
    }
    final revNotifier = _hashRevisions[hash];
    if (revNotifier != null) {
      revNotifier.value = revNotifier.value + 1;
    }
    final entry = _entryBySha256[hash];
    if (entry != null && !entry.isDisposed) {
      entry.visualChangeNotifier.notify();
    }
  }

  void _evictAndBumpHashes(List<String> recentHashes) {
    final imageCache = PaintingBinding.instance.imageCache;
    for (final rawHash in recentHashes) {
      final hash = rawHash.toLowerCase();
      final boundKey = _cacheKeysByHash[hash];
      if (boundKey != null) {
        imageCache.evict(boundKey, includeLive: true);
      }
      final revNotifier = _hashRevisions[hash];
      if (revNotifier != null) {
        revNotifier.value = revNotifier.value + 1;
      }
      final entry = _entryBySha256[hash];
      if (entry != null && !entry.isDisposed) {
        entry.visualChangeNotifier.notify();
      }
    }
  }

  ValueNotifier<int> revisionNotifierFor(String sha256) {
    final key = sha256.toLowerCase();
    return _hashRevisions.putIfAbsent(key, () => ValueNotifier<int>(0));
  }

  void bindImageCacheKey(String sha256, Object cacheKey) {
    _cacheKeysByHash[sha256.toLowerCase()] = cacheKey;
  }

  Future<Map<String, dynamic>?> refreshStatus() async {
    try {
      final raw = await _safChannel.invokeMapMethod<dynamic, dynamic>('getStatus');
      if (raw != null) {
        progressNotifier.value = CloudSyncProgress.fromMap(raw);
        return raw.cast<String, dynamic>();
      }
    } catch (_) {}
    return null;
  }

  Future<void> configureCloud({
    String? lanBaseUrl,
    String? lanApiToken,
    String? oauthClientId,
    String? oauthClientSecret,
    String? driveRootFolderName,
    String? driveRootFolderId,
  }) async {
    try {
      await _safChannel.invokeMethod('configureCloud', {
        if (lanBaseUrl != null) 'lanBaseUrl': lanBaseUrl,
        if (lanApiToken != null) 'lanApiToken': lanApiToken,
        if (oauthClientId != null) 'oauthClientId': oauthClientId,
        if (oauthClientSecret != null) 'oauthClientSecret': oauthClientSecret,
        if (driveRootFolderName != null) 'driveRootFolderName': driveRootFolderName,
        if (driveRootFolderId != null) 'driveRootFolderId': driveRootFolderId,
      });
    } catch (e) {
      debugPrint('CloudSyncService: configureCloud error: $e');
    }
  }

  Future<bool> authorizeGoogleDrive({String? clientId, String? clientSecret}) async {
    ensureInitialized();
    try {
      final ok = await _safChannel.invokeMethod<bool>('authorizeGoogleDrive', {
        if (clientId != null) 'clientId': clientId,
        if (clientSecret != null) 'clientSecret': clientSecret,
      });
      return ok ?? false;
    } catch (e) {
      debugPrint('CloudSyncService: authorizeGoogleDrive error: $e');
      return false;
    }
  }

  Future<void> clearDriveAuth() async {
    try {
      await _safChannel.invokeMethod('clearDriveAuth');
    } catch (e) {
      debugPrint('CloudSyncService: clearDriveAuth error: $e');
    }
  }

  /// 结合 sha256 与 relPath 计算稳定 53-bit 负数 ID，彻底消除重复内容覆盖与 JS 溢出
  int _stableCloudEntryId(String sha256, String relPath) {
    int hash = 0xcbf29ce484222325;
    for (final codeUnit in '$sha256|$relPath'.codeUnits) {
      hash ^= codeUnit;
      hash = (hash * 0x100000001b3) & 0x1FFFFFFFFFFFFF; // 53-bit safe int
    }
    return hash == 0 ? -1 : -hash;
  }

  /// 自动同步：调用 Kotlin 统一云端引擎（自动享受 LAN 450ms 极速通道与 Drive REST API v3 降级）
  Future<void> sync(CollectionSource source) async {
    if (_isSyncing) return;
    _isSyncing = true;
    ensureInitialized();
    try {
      // 冷启动索引修复：在开头无条件用 source.allEntries 重建 _entryBySha256 映射
      for (final e in source.allEntries) {
        if (e.origin == EntryOrigins.cloud) {
          final uri = Uri.tryParse(e.uri);
          final host = uri?.host.toLowerCase().trim() ?? '';
          if (host.length >= 16) {
            _entryBySha256[host] = e;
          }
          final thumb = uri?.queryParameters['thumb']?.replaceAll('.webp', '').toLowerCase().trim();
          if (thumb != null && thumb.length >= 16) {
            _entryBySha256[thumb] = e;
          }
        }
      }

      final localSnapshotVersion = settings.store.getInt('cloud_snapshot_version') ?? 0;
      debugPrint('CloudSyncService: invoking syncMasterSnapshot (localVer: $localSnapshotVersion)...');

      final res = await _safChannel.invokeMapMethod<String, dynamic>('syncMasterSnapshot', {
        'localSnapshotVersion': localSnapshotVersion,
      });

      if (res != null) {
        final snapshotPath = res['snapshotPath'] as String?;
        final remoteVersion = (res['snapshotVersion'] as num?)?.toInt() ?? 0;
        final channel = res['channel'] as String? ?? 'unknown';

        if (snapshotPath != null && File(snapshotPath).existsSync()) {
          final currentCached = source.allEntries.where((e) => e.origin == EntryOrigins.cloud).toSet();
          if (remoteVersion > 0 && remoteVersion == localSnapshotVersion && currentCached.isNotEmpty) {
            debugPrint('CloudSyncService: Snapshot v$localSnapshotVersion already up-to-date (channel: $channel)');
          } else {
            debugPrint('CloudSyncService: Loading snapshot v$remoteVersion from $snapshotPath (channel: $channel)...');
            await _loadSnapshotDatabase(File(snapshotPath), source);
            await settings.store.setInt('cloud_snapshot_version', remoteVersion);
          }
        }
      }

      unawaited(flushPendingOutbox());
      unawaited(performAutoBackup(source));
    } catch (e, stack) {
      debugPrint('CloudSyncService: sync failed: $e\n$stack');
    } finally {
      _isSyncing = false;
    }
  }

  Future<void> _loadSnapshotDatabase(File dbFile, CollectionSource source) async {
    final db = await openReadOnlyDatabase(dbFile.path);

    // 检查是否有分卷列，若无则容错查询
    List<Map<String, Object?>> rows;
    try {
      rows = await db.rawQuery(
        'SELECT id, relative_path, file_name, mime_type, file_size, width, height, duration_ms, taken_at_ms, modified_at_ms, thumb_rel_path, pack_id, pack_offset, pack_length FROM media_items WHERE COALESCE(is_deleted, 0) = 0',
      );
    } catch (_) {
      rows = await db.rawQuery(
        'SELECT id, relative_path, file_name, mime_type, file_size, width, height, duration_ms, taken_at_ms, modified_at_ms, thumb_rel_path, NULL as pack_id, NULL as pack_offset, NULL as pack_length FROM media_items WHERE COALESCE(is_deleted, 0) = 0',
      );
    }

    final List<Map<String, Object?>> deletedRows = await db.rawQuery(
      'SELECT id FROM media_items WHERE is_deleted = 1',
    );
    await db.close();

    debugPrint('CloudSyncService: parsed ${rows.length} active rows, ${deletedRows.length} deleted rows from snapshot');

    // 1. 先清理已被标记删除的图片
    _isPurgingDeletedEntries = true;
    try {
      final deletedIdSet = deletedRows
          .map((r) => (r['id'] as String? ?? '').toLowerCase().trim())
          .where((s) => s.isNotEmpty)
          .toSet();
      if (deletedIdSet.isNotEmpty) {
        final toRemoveUris = source.allEntries
            .where((e) => e.origin == EntryOrigins.cloud)
            .where((e) {
              final uri = Uri.tryParse(e.uri);
              if (uri == null) return false;
              final host = uri.host.toLowerCase().trim();
              if (deletedIdSet.contains(host)) return true;
              final thumb = uri.queryParameters['thumb']?.replaceAll('.webp', '').toLowerCase().trim();
              if (thumb != null && deletedIdSet.contains(thumb)) return true;
              return false;
            })
            .map((e) => e.uri)
            .toSet();
        if (toRemoveUris.isNotEmpty) {
          await source.removeEntries(toRemoveUris, includeTrash: true);
          debugPrint('CloudSyncService: purged ${toRemoveUris.length} deleted entries from gallery');
        }
      }
    } finally {
      _isPurgingDeletedEntries = false;
    }

    // 2. 本地现有条目指纹集合（用于对齐手机备份照片与云端去重，避免时间轴重复）
    final localSignatures = <String>{};
    for (final e in source.allEntries) {
      if (e.origin != EntryOrigins.cloud) {
        final name = (e.sourceTitle ?? (e.path != null ? p.basename(e.path!) : '')).toLowerCase();
        final size = e.sizeBytes ?? 0;
        if (name.isNotEmpty && size > 0) {
          localSignatures.add('$name:$size');
        }
      }
    }

    // 3. 构建 O(1) 现有条目映射
    final existingByUri = <String, AvesEntry>{
      for (final e in source.allEntries)
        if (e.origin == EntryOrigins.cloud) e.uri: e,
    };

    final cloudEntries = <AvesEntry>{};
    for (final row in rows) {
      final sha256 = row['id'] as String? ?? '';
      if (sha256.isEmpty) continue;
      final relPath = (row['relative_path'] as String? ?? '').replaceAll('\\', '/');
      if (relPath.isEmpty) continue;
      final fileName = row['file_name'] as String? ?? p.basename(relPath);
      final mimeType = row['mime_type'] as String? ?? 'image/jpeg';
      final fileSize = (row['file_size'] as num?)?.toInt() ?? 0;
      final width = (row['width'] as num?)?.toInt() ?? 0;
      final height = (row['height'] as num?)?.toInt() ?? 0;
      final durationMs = (row['duration_ms'] as num?)?.toInt() ?? 0;
      final takenAtMs = (row['taken_at_ms'] as num?)?.toInt() ?? DateTime.now().millisecondsSinceEpoch;
      final modifiedAtMs = (row['modified_at_ms'] as num?)?.toInt() ?? takenAtMs;
      final thumbRelPath = (row['thumb_rel_path'] as String?) ?? '';
      final thumbName = thumbRelPath.isNotEmpty ? thumbRelPath.split('/').last : '$sha256.webp';
      final thumbHash = thumbName.replaceAll('.webp', '');

      // 本地/云端去重：若为手机上传备份且本地已有同 (fileName, sizeBytes) 实体，跳过插入
      if (relPath.startsWith('手机/')) {
        final dedupKey = '${fileName.toLowerCase()}:$fileSize';
        if (localSignatures.contains(dedupKey)) {
          continue;
        }
      }

      final packId = row['pack_id'] as String?;
      final packOffset = row['pack_offset'] as num?;
      final packLength = row['pack_length'] as num?;
      final hasPack = packId != null && packId.isNotEmpty && packOffset != null && packLength != null && packLength > 0;

      // 53-bit 稳定负数防碰撞 ID
      final entryId = _stableCloudEntryId(sha256, relPath);

      // 云端条目统一规范 URI
      final queryParams = <String, String>{
        'rel': relPath,
        'thumb': thumbName,
        if (hasPack) ...{
          'pack': packId,
          'off': '$packOffset',
          'len': '$packLength',
        },
      };

      final uri = Uri(scheme: 'cloudgallery', host: sha256, queryParameters: queryParams).toString();
      final displayPath = '/云端相册/$relPath';

      final existingEntry = existingByUri[uri];
      if (existingEntry != null) {
        _entryBySha256[sha256.toLowerCase()] = existingEntry;
        if (thumbHash.isNotEmpty && thumbHash != sha256) {
          _entryBySha256[thumbHash.toLowerCase()] = existingEntry;
        }
        continue;
      }

      // 视频保底尺寸 1920x1080，解除 Aves 播放器 isEmpty 拦截
      final isVideo = mimeType.startsWith('video/');
      final effectiveWidth = (width > 0) ? width : (isVideo ? 1920 : 0);
      final effectiveHeight = (height > 0) ? height : (isVideo ? 1080 : 0);

      final entry = AvesEntry(
        id: entryId,
        uri: uri,
        path: displayPath,
        contentId: null,
        pageId: null,
        mimeType: mimeType,
        width: effectiveWidth,
        height: effectiveHeight,
        rotationDegrees: 0,
        sizeBytes: fileSize,
        sourceTitle: fileName,
        dateAddedSecs: takenAtMs ~/ 1000,
        dateModifiedMillis: modifiedAtMs,
        sourceDateTakenMillis: takenAtMs,
        durationMillis: durationMs > 0 ? durationMs : null,
        trashed: false,
        origin: EntryOrigins.cloud,
      );
      cloudEntries.add(entry);
      _entryBySha256[sha256.toLowerCase()] = entry;
      if (thumbHash.isNotEmpty && thumbHash != sha256) {
        _entryBySha256[thumbHash.toLowerCase()] = entry;
      }
    }

    if (cloudEntries.isNotEmpty) {
      await localMediaDb.insertEntries(cloudEntries);
      source.addEntries(cloudEntries);
      source.notifyAlbumsChanged();
      debugPrint('CloudSyncService: added ${cloudEntries.length} entries to localMediaDb and source');
    }
  }

  /// 用户在 UI 交互层显式确认删除照片时调用（默认安全移入回收站 .cloudgallery/trash/）
  Future<void> trashEntriesOnServer(Set<AvesEntry> entries) async {
    if (_isPurgingDeletedEntries) return;
    if (entries.isEmpty) return;

    final ops = <Map<String, Object?>>[];
    final now = DateTime.now().millisecondsSinceEpoch;

    for (final entry in entries) {
      String? relPath;
      String? sha256;

      if (entry.origin == EntryOrigins.cloud) {
        final uri = Uri.tryParse(entry.uri);
        if (uri != null) {
          sha256 = uri.host;
          relPath = uri.queryParameters['rel'] ?? uri.queryParameters['path'];
        }
        if (relPath == null && entry.path != null) {
          relPath = entry.path!.replaceFirst(RegExp(r'^/云端相册/'), '');
        }
      } else {
        // 手机本地拍摄的照片
        final date = entry.bestDate ?? DateTime.fromMillisecondsSinceEpoch(entry.sourceDateTakenMillis ?? 0);
        final yyyy = date.year.toString();
        final mm = date.month.toString().padLeft(2, '0');
        final dd = date.day.toString().padLeft(2, '0');
        final fileName = entry.path != null ? p.basename(entry.path!) : (entry.sourceTitle ?? 'image.jpg');
        relPath = '手机/$yyyy/$yyyy-$mm-$dd/$fileName';
      }

      if (relPath != null && relPath.isNotEmpty) {
        ops.add({
          'op_id': 'op_trash_${now}_${entry.id.abs()}',
          'action': 'TRASH',
          'target': relPath,
          if (sha256 != null && sha256.length >= 16) 'sha256': sha256,
          'timestamp_ms': now,
        });
      }
    }

    if (ops.isEmpty) return;
    await _submitOrQueueOutbox(ops);
  }

  Future<void> _submitOrQueueOutbox(List<Map<String, Object?>> ops) async {
    bool synced = false;
    try {
      final ok = await _safChannel.invokeMethod<bool>('submitOutboxOps', {
        'opsJson': jsonEncode(ops),
      });
      if (ok == true) {
        synced = true;
        debugPrint('CloudSyncService: submitted ${ops.length} ops via CloudDriveApiClient');
      }
    } catch (e) {
      debugPrint('CloudSyncService: submitOutboxOps error: $e');
    }

    if (!synced) {
      final existingJson = settings.store.getString('pending_outbox_ops');
      final List<dynamic> list = existingJson != null ? (jsonDecode(existingJson) as List<dynamic>) : [];
      list.addAll(ops);
      await settings.store.setString('pending_outbox_ops', jsonEncode(list));
      debugPrint('CloudSyncService: queued ${ops.length} ops locally (total pending: ${list.length})');
    }
  }

  /// 冲刷离线积累的删除与变更指令
  Future<void> flushPendingOutbox() async {
    final existingJson = settings.store.getString('pending_outbox_ops');
    if (existingJson == null || existingJson.isEmpty) return;

    try {
      final ok = await _safChannel.invokeMethod<bool>('submitOutboxOps', {
        'opsJson': existingJson,
      });
      if (ok == true) {
        await settings.store.remove('pending_outbox_ops');
        debugPrint('CloudSyncService: flushed pending outbox ops successfully');
      }
    } catch (_) {}
  }

  /// 自动读取手机本地新照片并备份至电脑 (按日期自动整理归档)
  Future<void> performAutoBackup(CollectionSource source) async {
    if (_isAutoBackingUp) return;
    _isAutoBackingUp = true;
    try {
      await flushPendingOutbox();

      final localEntries = source.allEntries
          .where((e) => e.origin != EntryOrigins.cloud)
          .where((e) {
            final pLower = e.path?.toLowerCase() ?? '';
            final dLower = e.directory?.toLowerCase() ?? '';
            return pLower.contains('dcim/camera') || dLower.contains('camera') || dLower.contains('dcim');
          })
          .toList();

      if (localEntries.isEmpty) return;

      final backedUpList = settings.store.getStringList('backed_up_local_files') ?? [];
      final backedUpFingerprints = backedUpList.toSet();

      final toBackup = <AvesEntry>[];
      for (final entry in localEntries) {
        final filePath = entry.path;
        if (filePath == null) continue;
        final file = File(filePath);
        if (!file.existsSync()) continue;

        final fileLen = file.lengthSync();
        final date = entry.bestDate ?? DateTime.fromMillisecondsSinceEpoch(entry.sourceDateTakenMillis ?? 0);
        final yyyy = date.year.toString();
        final mm = date.month.toString().padLeft(2, '0');
        final dd = date.day.toString().padLeft(2, '0');
        final fileName = p.basename(filePath);
        final relPath = '手机/$yyyy/$yyyy-$mm-$dd/$fileName';
        final dedupKey = '$relPath:$fileLen';

        if (backedUpFingerprints.contains(dedupKey)) continue;
        toBackup.add(entry);
      }

      if (toBackup.isEmpty) {
        debugPrint('Auto-backup: all ${localEntries.length} local camera photos are already backed up');
        return;
      }

      debugPrint('Auto-backup: found ${toBackup.length} new photos to upload to cloud');

      int uploaded = 0;
      for (final entry in toBackup) {
        final filePath = entry.path;
        if (filePath == null) continue;
        final file = File(filePath);
        if (!file.existsSync()) continue;

        final fileLen = file.lengthSync();
        final date = entry.bestDate ?? DateTime.fromMillisecondsSinceEpoch(entry.sourceDateTakenMillis ?? 0);
        final yyyy = date.year.toString();
        final mm = date.month.toString().padLeft(2, '0');
        final dd = date.day.toString().padLeft(2, '0');
        final fileName = p.basename(filePath);
        final relPath = '手机/$yyyy/$yyyy-$mm-$dd/$fileName';
        final dedupKey = '$relPath:$fileLen';

        try {
          final ok = await _safChannel.invokeMethod<bool>('uploadBackupFile', {
            'localPath': filePath,
            'remoteRelPath': relPath,
            'mimeType': entry.mimeType,
          });

          if (ok == true) {
            uploaded++;
            backedUpFingerprints.add(dedupKey);
            if (uploaded % 10 == 0) {
              await settings.store.setStringList('backed_up_local_files', backedUpFingerprints.toList());
            }
            debugPrint('Auto-backup: [$uploaded/${toBackup.length}] uploaded $fileName -> $relPath');
          }
        } catch (e) {
          debugPrint('Auto-backup: failed to upload $fileName: $e');
        }
      }

      if (uploaded > 0) {
        await settings.store.setStringList('backed_up_local_files', backedUpFingerprints.toList());
        debugPrint('Auto-backup: successfully backed up $uploaded photos, refreshing snapshot');
        await sync(source);
      }
    } catch (e) {
      debugPrint('Auto-backup: error: $e');
    } finally {
      _isAutoBackingUp = false;
    }
  }
}
