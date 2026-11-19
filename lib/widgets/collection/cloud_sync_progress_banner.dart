import 'dart:async';

import 'package:aves/services/cloud_sync_service.dart';
import 'package:material_ui/material_ui.dart';

/// 顶部非侵入式云端缩略图同步进度横幅 (CloudSyncProgressBanner)
///
/// 特性：
/// 1. 实时展示流量：已下载 / 总字节数 (如 45.2 MB / 270.8 MB)；
/// 2. 实时展示张数：已解压 / 总张数 (如 1,230 / 9,007 张)；
/// 3. 前台视口抢占时显示状态：“优先加载当前画面”；
/// 4. 支持点击右上角折叠为极简胶囊条 / 展开；
/// 5. 全部解包完成后展示“云端高清缓存已就绪”，2.5 秒后优雅淡出收起，零打扰。
class CloudSyncProgressBanner extends StatefulWidget {
  final CloudSyncService service;
  final EdgeInsetsGeometry margin;

  CloudSyncProgressBanner({
    super.key,
    CloudSyncService? service,
    this.margin = const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
  }) : service = service ?? CloudSyncService.instance;

  @override
  State<CloudSyncProgressBanner> createState() => _CloudSyncProgressBannerState();
}

class _CloudSyncProgressBannerState extends State<CloudSyncProgressBanner> {
  bool _collapsed = false;
  bool _showCompletedToast = false;
  bool _fadedOut = false;
  bool _wasSyncing = false;
  Timer? _dismissTimer;

  @override
  void initState() {
    super.initState();
    widget.service.ensureInitialized();
    widget.service.progressNotifier.addListener(_onProgressChanged);
    _onProgressChanged();
  }

  @override
  void dispose() {
    widget.service.progressNotifier.removeListener(_onProgressChanged);
    _dismissTimer?.cancel();
    super.dispose();
  }

  void _onProgressChanged() {
    final p = widget.service.progressNotifier.value;
    if (p.isSyncing) {
      _wasSyncing = true;
      _dismissTimer?.cancel();
      if (_showCompletedToast || _fadedOut) {
        setState(() {
          _showCompletedToast = false;
          _fadedOut = false;
        });
      }
    } else if (p.isCompleted && _wasSyncing) {
      _wasSyncing = false;
      setState(() {
        _collapsed = false;
        _showCompletedToast = true;
        _fadedOut = false;
      });
      _dismissTimer?.cancel();
      _dismissTimer = Timer(const Duration(milliseconds: 2500), () {
        if (!mounted) return;
        setState(() => _fadedOut = true);
        _dismissTimer = Timer(const Duration(milliseconds: 400), () {
          if (!mounted) return;
          setState(() => _showCompletedToast = false);
        });
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<CloudSyncProgress>(
      valueListenable: widget.service.progressNotifier,
      builder: (context, progress, _) {
        final visible = progress.isSyncing || _showCompletedToast;
        if (!visible) {
          return const SizedBox.shrink();
        }

        final theme = Theme.of(context);
        final scheme = theme.colorScheme;

        return AnimatedOpacity(
          duration: const Duration(milliseconds: 350),
          curve: Curves.easeOut,
          opacity: _fadedOut ? 0.0 : 1.0,
          child: AnimatedSize(
            duration: const Duration(milliseconds: 240),
            curve: Curves.easeInOut,
            child: Container(
              margin: widget.margin,
              decoration: BoxDecoration(
                color: scheme.surfaceContainerHighest.withValues(alpha: 0.96),
                borderRadius: BorderRadius.circular(12),
                border: Border.all(
                  color: progress.isPreempted
                      ? scheme.primary.withValues(alpha: 0.75)
                      : scheme.outlineVariant.withValues(alpha: 0.5),
                  width: progress.isPreempted ? 1.5 : 1.0,
                ),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withValues(alpha: 0.08),
                    blurRadius: 8,
                    offset: const Offset(0, 2),
                  ),
                ],
              ),
              child: Material(
                color: Colors.transparent,
                child: _collapsed
                    ? _buildCollapsedBar(context, progress)
                    : _buildExpandedBar(context, progress),
              ),
            ),
          ),
        );
      },
    );
  }

  Widget _buildCollapsedBar(BuildContext context, CloudSyncProgress p) {
    final theme = Theme.of(context);
    final scheme = theme.colorScheme;
    return InkWell(
      borderRadius: BorderRadius.circular(12),
      onTap: () => setState(() => _collapsed = false),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        child: Row(
          children: [
            Icon(
              p.isCompleted ? Icons.check_circle_rounded : Icons.cloud_sync_rounded,
              size: 18,
              color: p.isCompleted ? Colors.green : scheme.primary,
            ),
            const SizedBox(width: 8),
            Expanded(
              child: Text(
                p.isCompleted
                    ? '云端高清缓存已就绪'
                    : p.isPreempted
                        ? '优先加载当前画面 · ${p.percentageLabel}'
                        : '缩略图解包中 · ${p.extractedCount}/${p.totalCount} (${p.percentageLabel})',
                style: theme.textTheme.bodySmall?.copyWith(
                  fontWeight: FontWeight.w600,
                ),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
              ),
            ),
            const SizedBox(width: 8),
            SizedBox(
              width: 56,
              child: LinearProgressIndicator(
                value: p.isCompleted ? 1.0 : p.progress,
                minHeight: 4,
                borderRadius: BorderRadius.circular(2),
              ),
            ),
            const SizedBox(width: 6),
            Icon(Icons.expand_more_rounded, size: 18, color: scheme.onSurfaceVariant),
          ],
        ),
      ),
    );
  }

  Widget _buildExpandedBar(BuildContext context, CloudSyncProgress p) {
    final theme = Theme.of(context);
    final scheme = theme.colorScheme;

    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 10, 8, 10),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(
                p.isCompleted ? Icons.check_circle_rounded : Icons.downloading_rounded,
                size: 20,
                color: p.isCompleted ? Colors.green : scheme.primary,
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Wrap(
                  spacing: 8,
                  runSpacing: 4,
                  crossAxisAlignment: WrapCrossAlignment.center,
                  children: [
                    Text(
                      p.isCompleted ? '云端高清缓存已就绪' : '后台解包缩略图',
                      style: theme.textTheme.bodyMedium?.copyWith(fontWeight: FontWeight.w600),
                    ),
                    if (p.isPreempted && !p.isCompleted)
                      Container(
                        padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                        decoration: BoxDecoration(
                          color: scheme.primaryContainer,
                          borderRadius: BorderRadius.circular(6),
                        ),
                        child: Text(
                          '优先加载当前画面',
                          style: theme.textTheme.labelSmall?.copyWith(
                            color: scheme.onPrimaryContainer,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                  ],
                ),
              ),
              Text(
                p.isCompleted ? '100.0%' : p.percentageLabel,
                style: theme.textTheme.labelLarge?.copyWith(
                  color: scheme.primary,
                  fontWeight: FontWeight.bold,
                ),
              ),
              IconButton(
                visualDensity: VisualDensity.compact,
                tooltip: '折叠进度条',
                icon: Icon(Icons.expand_less_rounded, size: 20, color: scheme.onSurfaceVariant),
                onPressed: () => setState(() => _collapsed = true),
              ),
            ],
          ),
          const SizedBox(height: 6),
          ClipRRect(
            borderRadius: BorderRadius.circular(4),
            child: LinearProgressIndicator(
              value: p.isCompleted ? 1.0 : p.progress,
              minHeight: 5,
              backgroundColor: scheme.surfaceContainerLow,
            ),
          ),
          const SizedBox(height: 6),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text(
                '流量：${p.formattedSizePair}',
                style: theme.textTheme.bodySmall?.copyWith(color: scheme.onSurfaceVariant),
              ),
              Text(
                '已解压：${p.extractedCount} / ${p.totalCount} 张',
                style: theme.textTheme.bodySmall?.copyWith(color: scheme.onSurfaceVariant),
              ),
            ],
          ),
        ],
      ),
    );
  }
}
