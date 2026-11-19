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
import 'package:http/http.dart' as http;
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

  // ignore: unnecessary_type_name_in_constructor
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

  // ignore: unnecessary_type_name_in_constructor
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

  String get formattedSizePair {
    return '${_formatBytes(downloadedBytes)} / ${_formatBytes(totalBytes)}';
  }

  String get percentageLabel {
    return '${(progress * 100).clamp(0, 100).toStringAsFixed(1)}%';
  }

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

  // ignore: unnecessary_type_name_in_constructor
  CloudSyncService._();

  void dispose() {
    _eventSubscription?.cancel();
    _eventSubscription = null;
    _initialized = false;
  }

  String get defaultHost => 'http://192.168.5.3:8088';

  String get serverUrl => (settings.store.getString('cloud_server_url') ?? defaultHost).trim().replaceAll(RegExp(r'/+$'), '');

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
      if (sha256 != null && sha256.length == 64) {
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
    final imageCache = PaintingBinding.instance.imageCache;
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

  Future<void> refreshStatus() async {
    try {
      final raw = await _safChannel.invokeMapMethod<dynamic, dynamic>('getStatus');
      if (raw != null) {
        progressNotifier.value = CloudSyncProgress.fromMap(raw);
      }
    } catch (_) {}
  }

  Future<String?> getPersistedTreeUri() async {
    try {
      return await _safChannel.invokeMethod<String>('getPersistedTreeUri');
    } catch (e) {
      debugPrint('CloudSyncService: getPersistedTreeUri error: $e');
      return null;
    }
  }

  Future<bool> startThumbPackUnpack({String? treeUri, bool force = false}) async {
    ensureInitialized();
    try {
      final res = await _safChannel.invokeMethod<bool>('startThumbPackUnpack', {
        'treeUri': treeUri,
        'force': force,
      });
      return res ?? false;
    } catch (e) {
      debugPrint('CloudSyncService: startThumbPackUnpack error: $e');
      return false;
    }
  }

  Future<bool> setRateLimitKbps(int kbps) async {
    try {
      final res = await _safChannel.invokeMethod<bool>('setRateLimitKbps', {
        'rateLimitKbps': kbps,
      });
      return res ?? false;
    } catch (e) {
      debugPrint('CloudSyncService: setRateLimitKbps error: $e');
      return false;
    }
  }

  /// 唤起系统文件夹选择器，选择 Google 云端硬盘相册目录 (SAF TreeUri) 并秒级同步元数据快照
  Future<bool> mountGoogleDriveSaf(CollectionSource source) async {
    if (_isSyncing) return false;
    _isSyncing = true;
    ensureInitialized();
    try {
      debugPrint('CloudSyncService: invoking mountGoogleDrive via SAF...');
      final result = await _safChannel.invokeMapMethod<String, dynamic>('mountGoogleDrive');
      if (result == null) {
        debugPrint('CloudSyncService: mountGoogleDrive returned null');
        return false;
      }

      final snapshotPath = result['snapshotPath'] as String?;
      final treeUri = result['treeUri'] as String?;
      if (snapshotPath == null || !File(snapshotPath).existsSync()) {
        debugPrint('CloudSyncService: snapshot path missing or file not found: $snapshotPath');
        return false;
      }

      debugPrint('CloudSyncService: Google Drive SAF mounted successfully: $treeUri, loading snapshot: $snapshotPath');
      await _loadSnapshotDatabase(File(snapshotPath), source, isSaf: true);
      return true;
    } catch (e, stack) {
      debugPrint('CloudSyncService: mountGoogleDriveSaf error: $e\n$stack');
      return false;
    } finally {
      _isSyncing = false;
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

  /// 自动同步：优先 800ms 探测局域网 PC Daemon，不可达再自动降级 Google Drive SAF
  Future<void> sync(CollectionSource source) async {
    if (_isSyncing) return;
    _isSyncing = true;
    ensureInitialized();
    try {
      final lanOk = await _trySyncViaLanHttp(source);
      if (!lanOk) {
        await _trySyncViaGoogleDriveSaf(source);
      }
      unawaited(flushPendingOutbox());
      unawaited(performAutoBackup(source));
    } catch (e, stack) {
      debugPrint('CloudSyncService: sync failed: $e\n$stack');
    } finally {
      _isSyncing = false;
    }
  }

  Future<bool> _trySyncViaLanHttp(CollectionSource source) async {
    final baseUrl = serverUrl;
    try {
      final pingUri = Uri.parse('$baseUrl/api/version');
      final resp = await http.get(pingUri).timeout(const Duration(milliseconds: 800));
      if (resp.statusCode != 200) return false;

      final manifest = jsonDecode(resp.body) as Map<String, dynamic>;
      final remoteSnapshotVersion = (manifest['snapshot_version'] as num?)?.toInt() ?? 0;
      final localSnapshotVersion = settings.store.getInt('cloud_snapshot_version') ?? 0;

      final currentCached = await localMediaDb.loadEntries(origin: EntryOrigins.cloud);
      if (remoteSnapshotVersion == localSnapshotVersion && currentCached.isNotEmpty) {
        debugPrint('CloudSyncService: LAN snapshot v$localSnapshotVersion already up-to-date');
        return true;
      }

      debugPrint('CloudSyncService: downloading LAN HTTP snapshot (remote v$remoteSnapshotVersion)...');
      final dbUri = Uri.parse('$baseUrl/api/database');
      final dbResp = await http.get(dbUri).timeout(const Duration(seconds: 30));
      if (dbResp.statusCode != 200) return false;

      final dbDir = await getDatabasesPath();
      final tempDbFile = File(p.join(dbDir, 'master_index_snapshot_http.db'));
      await tempDbFile.writeAsBytes(dbResp.bodyBytes, flush: true);

      await _loadSnapshotDatabase(tempDbFile, source, isSaf: false, baseUrl: baseUrl);
      try {
        await tempDbFile.delete();
      } catch (_) {}

      await settings.store.setInt('cloud_snapshot_version', remoteSnapshotVersion);
      return true;
    } catch (e) {
      debugPrint('CloudSyncService: LAN HTTP sync probe skipped/unreachable: $e');
      return false;
    }
  }

  Future<bool> _trySyncViaGoogleDriveSaf(CollectionSource source) async {
    final treeUri = await getPersistedTreeUri();
    if (treeUri == null || treeUri.isEmpty) return false;

    debugPrint('CloudSyncService: attempting Google Drive SAF fallback sync: $treeUri');
    try {
      final res = await _safChannel.invokeMapMethod<String, dynamic>('syncSnapshot');
      final path = res?['snapshotPath'] as String?;
      if (path != null && File(path).existsSync()) {
        await _loadSnapshotDatabase(File(path), source, isSaf: true);
        unawaited(startThumbPackUnpack(treeUri: treeUri));
        return true;
      }
    } catch (e) {
      debugPrint('CloudSyncService: SAF syncSnapshot error: $e');
    }
    unawaited(startThumbPackUnpack(treeUri: treeUri));
    return false;
  }

  Future<void> _loadSnapshotDatabase(
    File dbFile,
    CollectionSource source, {
    required bool isSaf,
    String? baseUrl,
  }) async {
    final db = await openReadOnlyDatabase(dbFile.path);
    final List<Map<String, Object?>> rows = await db.rawQuery(
      'SELECT id, relative_path, file_name, mime_type, file_size, width, height, duration_ms, taken_at_ms, modified_at_ms, thumb_rel_path FROM media_items WHERE COALESCE(is_deleted, 0) = 0',
    );
    final List<Map<String, Object?>> deletedRows = await db.rawQuery(
      'SELECT id FROM media_items WHERE is_deleted = 1',
    );
    await db.close();

    debugPrint('CloudSyncService: parsed ${rows.length} active rows, ${deletedRows.length} deleted rows from snapshot (isSaf=$isSaf)');

    // 1. 先彻底清理已被标记删除的图片 (包括 SAF 虚拟 URI 与 LAN HTTP URI)
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

    // 构建 O(1) 现有条目映射，彻底消除 O(N^2) 循环内 firstWhere 查重
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

      // 53-bit 稳定负数防碰撞 ID
      final entryId = _stableCloudEntryId(sha256, relPath);

      final encodedPath = Uri.encodeComponent(relPath);

      final String uri;
      final String displayPath;

      if (isSaf) {
        // 原生 Google Drive SAF 虚拟协议 URI
        final thumbQuery = thumbHash.isNotEmpty && thumbHash != sha256 ? '&thumb=$thumbHash' : '';
        uri = 'cloudgallery://$sha256?rel=$encodedPath$thumbQuery';
        displayPath = '/Google云端硬盘/$relPath';
      } else {
        // 小电脑 HTTP 局域网通道 URI
        uri = '$baseUrl/api/original?path=$encodedPath&thumb=$thumbName';
        displayPath = '/局域网云端相册/$relPath';
      }

      final existingEntry = existingByUri[uri];
      if (existingEntry != null) {
        _entryBySha256[sha256.toLowerCase()] = existingEntry;
        if (thumbHash.isNotEmpty && thumbHash != sha256) {
          _entryBySha256[thumbHash.toLowerCase()] = existingEntry;
        }
        continue;
      }

      // 关键修复：若为视频且宽高为 0，赋予保底尺寸 1920x1080，解除 Aves 播放器 isEmpty 拦截
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

  /// 手机端删除照片时的双向删除同步 (Delete Sync)
  /// 当用户在手机上删除照片时（无论来自云端还是已备份的本地原图），
  /// 自动生成操作指令投递至小电脑 PC Daemon，实现电脑端无感同步清理物理文件，避免二次整理。
  Future<void> onEntriesDeleted(Set<AvesEntry> entries) async {
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
          relPath = entry.path!
              .replaceFirst(RegExp(r'^/Google云端硬盘/'), '')
              .replaceFirst(RegExp(r'^/局域网云端相册/'), '');
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
          'op_id': 'op_del_${now}_${entry.id.abs()}',
          'action': 'DELETE_PERMANENT',
          'target': relPath,
          if (sha256 != null && sha256.length == 64) 'sha256': sha256,
          'timestamp_ms': now,
        });
      }
    }

    if (ops.isEmpty) return;

    // 优先尝试局域网直连投递
    bool synced = false;
    try {
      final outboxUri = Uri.parse('$serverUrl/api/outbox');
      final resp = await http.post(
        outboxUri,
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode(ops),
      ).timeout(const Duration(seconds: 4));

      if (resp.statusCode == 200) {
        synced = true;
        debugPrint('Delete sync: successfully sent ${ops.length} deletion ops to PC daemon');
      }
    } catch (e) {
      debugPrint('Delete sync: PC daemon unreachable: $e');
    }

    // 离线状态暂存至本地队列
    if (!synced) {
      await _queuePendingOutbox(ops);
    }
  }

  Future<void> _queuePendingOutbox(List<Map<String, Object?>> ops) async {
    final existingJson = settings.store.getString('pending_outbox_ops');
    final List<dynamic> list = existingJson != null ? (jsonDecode(existingJson) as List<dynamic>) : [];
    list.addAll(ops);
    await settings.store.setString('pending_outbox_ops', jsonEncode(list));
    debugPrint('Delete sync: queued ${ops.length} ops locally (total pending: ${list.length})');
  }

  /// 冲刷离线积累的删除与变更指令
  Future<void> flushPendingOutbox() async {
    final existingJson = settings.store.getString('pending_outbox_ops');
    if (existingJson == null || existingJson.isEmpty) return;

    try {
      final outboxUri = Uri.parse('$serverUrl/api/outbox');
      final resp = await http.post(
        outboxUri,
        headers: {'Content-Type': 'application/json'},
        body: existingJson,
      ).timeout(const Duration(seconds: 5));

      if (resp.statusCode == 200) {
        await settings.store.remove('pending_outbox_ops');
        debugPrint('Delete sync: flushed pending outbox ops successfully');
      }
    } catch (_) {}
  }

  /// 自动读取手机本地新照片并备份至电脑 (按日期自动整理归档)
  Future<void> performAutoBackup(CollectionSource source) async {
    if (_isAutoBackingUp) return;
    _isAutoBackingUp = true;
    try {
      final baseUrl = serverUrl;
      try {
        final ping = await http.get(Uri.parse('$baseUrl/api/ping')).timeout(const Duration(seconds: 2));
        if (ping.statusCode != 200) return;
      } catch (_) {
        return;
      }

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
      final backedUpFiles = backedUpList.toSet();

      // 提取所有云端已有文件名避免重复备份
      final cloudFilenames = source.allEntries
          .where((e) => e.origin == EntryOrigins.cloud)
          .map((e) => e.sourceTitle)
          .nonNulls
          .toSet();
      backedUpFiles.addAll(cloudFilenames);

      final toBackup = <AvesEntry>[];
      for (final entry in localEntries) {
        final fileName = entry.path != null ? p.basename(entry.path!) : (entry.sourceTitle ?? '');
        if (fileName.isEmpty || backedUpFiles.contains(fileName)) continue;
        toBackup.add(entry);
      }

      if (toBackup.isEmpty) {
        debugPrint('Auto-backup: all ${localEntries.length} local camera photos are already backed up');
        return;
      }

      debugPrint('Auto-backup: found ${toBackup.length} new photos to upload to PC daemon');

      int uploaded = 0;
      for (final entry in toBackup) {
        final filePath = entry.path;
        if (filePath == null) continue;
        final file = File(filePath);
        if (!await file.exists()) continue;

        final fileLen = await file.length();
        final date = entry.bestDate ?? DateTime.fromMillisecondsSinceEpoch(entry.sourceDateTakenMillis ?? 0);
        final yyyy = date.year.toString();
        final mm = date.month.toString().padLeft(2, '0');
        final dd = date.day.toString().padLeft(2, '0');
        final fileName = p.basename(filePath);
        final relPath = '手机/$yyyy/$yyyy-$mm-$dd/$fileName';
        final dedupKey = '$relPath:$fileLen';

        if (backedUpFiles.contains(dedupKey) || backedUpFiles.contains(fileName)) {
          continue;
        }

        try {
          final uploadUri = Uri.parse('$baseUrl/api/upload?path=${Uri.encodeComponent(relPath)}&op_type=BACKUP_PHOTO');
          final req = http.StreamedRequest('POST', uploadUri)
            ..headers.addAll({
              'Content-Type': 'application/octet-stream',
              'x-relative-path': Uri.encodeComponent(relPath),
              'x-op-type': 'BACKUP_PHOTO',
            })
            ..contentLength = fileLen;

          unawaited(file.openRead().pipe(req.sink));
          final streamedResp = await req.send().timeout(const Duration(minutes: 2));
          final resp = await http.Response.fromStream(streamedResp);

          if (resp.statusCode == 200) {
            uploaded++;
            backedUpFiles.add(dedupKey);
            backedUpFiles.add(fileName);
            await settings.store.setStringList('backed_up_local_files', backedUpFiles.toList());
            debugPrint('Auto-backup: [$uploaded/${toBackup.length}] uploaded $fileName -> $relPath');
          }
        } catch (e) {
          debugPrint('Auto-backup: failed to upload $fileName: $e');
        }
      }

      if (uploaded > 0) {
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
