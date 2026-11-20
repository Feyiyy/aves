import 'package:material_ui/material_ui.dart';

/// 顶部云端同步状态横幅 (CloudSyncProgressBanner)
class CloudSyncProgressBanner extends StatelessWidget {
  final EdgeInsetsGeometry margin;

  const CloudSyncProgressBanner({
    super.key,
    dynamic service,
    this.margin = const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
  });

  @override
  Widget build(BuildContext context) {
    return const SizedBox.shrink();
  }
}
