import 'package:aves/model/filters/container/album_group.dart';
import 'package:aves/model/filters/covered/stored_album.dart';
import 'package:aves/model/filters/trash.dart';
import 'package:aves/model/settings/settings.dart';
import 'package:aves/model/source/album.dart';
import 'package:aves/model/source/collection_lens.dart';
import 'package:aves/model/source/collection_source.dart';
import 'package:aves/model/source/location/country.dart';
import 'package:aves/model/source/location/place.dart';
import 'package:aves/model/source/tag.dart';
import 'package:aves/services/cloud_sync_service.dart';
import 'package:aves/services/common/services.dart';
import 'package:aves/widgets/common/action_mixins/feedback.dart';
import 'package:aves/theme/durations.dart';
import 'package:aves/theme/icons.dart';
import 'package:aves/theme/styles.dart';
import 'package:aves/utils/android_file_utils.dart';
import 'package:aves/utils/file_utils.dart';
import 'package:aves/widgets/about/about_page.dart';
import 'package:aves/widgets/collection/collection_page.dart';
import 'package:aves/widgets/common/basic/text/outlined.dart';
import 'package:aves/widgets/common/extensions/build_context.dart';
import 'package:aves/widgets/common/extensions/media_query.dart';
import 'package:aves/widgets/common/identity/aves_logo.dart';
import 'package:aves/widgets/debug/app_debug_page.dart';
import 'package:aves/widgets/explorer/page.dart';
import 'package:aves/widgets/filter_grids/albums_page.dart';
import 'package:aves/widgets/filter_grids/countries_page.dart';
import 'package:aves/widgets/filter_grids/places_page.dart';
import 'package:aves/widgets/filter_grids/tags_page.dart';
import 'package:aves/widgets/home/home_page.dart';
import 'package:aves/widgets/navigation/drawer/collection_nav_tile.dart';
import 'package:aves/widgets/navigation/drawer/page_nav_tile.dart';
import 'package:aves/widgets/navigation/drawer/tile.dart';
import 'package:aves/widgets/navigation/nav_item.dart';
import 'package:aves/widgets/settings/settings_page.dart';
import 'package:aves_model/aves_model.dart';
import 'package:aves_utils/aves_utils.dart';
import 'package:collection/collection.dart';
import 'package:flutter/foundation.dart';
import 'package:material_ui/material_ui.dart';
import 'package:provider/provider.dart';

class const AppDrawer({
  super.key,
  final CollectionLens? currentCollection,
  final String? currentExplorerPath,
}) extends StatefulWidget {
  // collection loaded in the `CollectionPage`, if any
  // current path loaded in the `ExplorerPage`, if any
  @override
  State<AppDrawer> createState() => _AppDrawerState();

  static List<AlbumBaseFilter> _getDefaultAlbums(BuildContext context) {
    final source = context.read<CollectionSource>();
    final specialAlbums = source.rawAlbums.where((album) {
      final type = androidFileUtils.getAlbumType(album);
      return [AlbumType.camera, AlbumType.download, AlbumType.screenshots].contains(type);
    }).toList()..sort(source.compareAlbumsByName);
    return specialAlbums.map((v) => StoredAlbumFilter(v, source.getStoredAlbumDisplayName(context, v))).toList();
  }

  static List<AlbumBaseFilter>? _getCustomAlbums(BuildContext context) {
    final source = context.read<CollectionSource>();
    return settings.drawerAlbumBookmarks?.map((v) {
      if (v is StoredAlbumFilter) {
        final album = v.album;
        return StoredAlbumFilter(album, source.getStoredAlbumDisplayName(context, album));
      }
      return v;
    }).toList();
  }

  static List<AlbumBaseFilter> effectiveAlbumBookmarks(BuildContext context) {
    return _getCustomAlbums(context) ?? _getDefaultAlbums(context);
  }
}

class _AppDrawerState extends State<AppDrawer> with WidgetsBindingObserver, FeedbackMixin {
  // using the default controller conflicts
  // with bottom nav bar primary scroll monitoring
  final ScrollController _scrollController = ScrollController();
  late Future<List<Object>> _profileSwitchFuture;
  bool _profileSwitchPermissionRequested = false;

  CollectionLens? get currentCollection => widget.currentCollection;

  @override
  void initState() {
    super.initState();
    _initProfileSwitchFuture();
    WidgetsBinding.instance.addObserver(this);
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _scrollController.dispose();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == .resumed && _profileSwitchPermissionRequested) {
      _profileSwitchPermissionRequested = false;
      _initProfileSwitchFuture();
      setState(() {});
    }
  }

  void _initProfileSwitchFuture() {
    _profileSwitchFuture = Future.wait([
      appProfileService.canRequestInteractAcrossProfiles(),
      appProfileService.canInteractAcrossProfiles(),
      appProfileService.getProfileSwitchingLabel(),
    ]);
  }

  @override
  Widget build(BuildContext context) {
    final drawerItems = <Widget>[
      _buildHeader(context),
      _buildHomeLink(),
      ..._buildTypeLinks(),
      _buildAlbumLinks(context),
      ..._buildPageLinks(context),
      if (settings.enableBin) ...[
        const Divider(),
        binTile(context),
      ],
      if (!kReleaseMode) ...[
        const Divider(),
        debugTile,
      ],
    ];

    return Drawer(
      child: ListTileTheme.merge(
        selectedColor: Theme.of(context).colorScheme.primary,
        horizontalTitleGap: 20,
        visualDensity: VisualDensity.comfortable,
        child: Selector<MediaQueryData, double>(
          selector: (context, mq) => mq.safeBottomPadding,
          builder: (context, safeBottomPadding, child) {
            final textScaler = MediaQuery.textScalerOf(context);
            final iconTheme = IconTheme.of(context);
            return SingleChildScrollView(
              controller: _scrollController,
              // key is expected by test driver
              key: const Key('drawer-scrollview'),
              padding: EdgeInsets.only(bottom: safeBottomPadding),
              child: IconTheme(
                data: iconTheme.copyWith(
                  size: textScaler.scale(iconTheme.size!),
                ),
                child: Column(
                  children: drawerItems,
                ),
              ),
            );
          },
        ),
      ),
    );
  }

  Widget _buildHeader(BuildContext context) {
    final l10n = context.l10n;
    final appName = l10n.appName;

    Future<void> goTo(String routeName, WidgetBuilder pageBuilder) async {
      Navigator.maybeOf(context)?.pop();
      await Future.pause(ADurations.drawerTransitionLoose);
      await Navigator.maybeOf(context)?.push(
        MaterialPageRoute(
          settings: RouteSettings(name: routeName),
          builder: pageBuilder,
        ),
      );
    }

    final colorScheme = Theme.of(context).colorScheme;
    final onPrimary = colorScheme.onPrimary;

    final drawerButtonStyle = ButtonStyle(
      padding: WidgetStateProperty.all(const EdgeInsetsDirectional.only(start: 12, end: 16)),
    );
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      color: colorScheme.primary,
      child: SafeArea(
        bottom: false,
        child: OutlinedButtonTheme(
          data: OutlinedButtonThemeData(
            style: ButtonStyle(
              foregroundColor: WidgetStateProperty.all<Color>(onPrimary),
              overlayColor: WidgetStateProperty.all<Color>(onPrimary.withValues(alpha: .12)),
              iconColor: WidgetStateProperty.all<Color>(onPrimary),
              side: WidgetStateProperty.all<BorderSide>(BorderSide(width: 1, color: onPrimary.withValues(alpha: .24))),
            ),
          ),
          child: Column(
            crossAxisAlignment: .start,
            children: [
              const SizedBox(height: 6),
              Align(
                alignment: .centerStart,
                child: Wrap(
                  spacing: 16,
                  crossAxisAlignment: .center,
                  children: [
                    const AvesLogo(size: 48),
                    OutlinedText(
                      textSpans: [
                        TextSpan(
                          text: appName,
                          style: AStyles.appNameStyle(context, appName).copyWith(
                            color: Colors.white,
                            fontSize: 38,
                            fontWeight: .w300,
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 8),
              Wrap(
                spacing: 8,
                children: [
                  OutlinedButton.icon(
                    key: const Key('drawer-cloud-button'),
                    onPressed: () {
                      final source = context.read<CollectionSource>();
                      _showCloudSettingsDialog(context, source);
                    },
                    style: drawerButtonStyle,
                    icon: const Icon(Icons.cloud_outlined),
                    label: const Text('云端设置与同步'),
                  ),
                  OutlinedButton.icon(
                    // key is expected by test driver
                    key: const Key('drawer-about-button'),
                    onPressed: () => goTo(AboutPage.routeName, (_) => const AboutPage()),
                    style: drawerButtonStyle,
                    icon: const Icon(AIcons.info),
                    label: Text(l10n.drawerAboutButton),
                  ),
                  OutlinedButton.icon(
                    // key is expected by test driver
                    key: const Key('drawer-settings-button'),
                    onPressed: () => goTo(SettingsPage.routeName, (_) => const SettingsPage()),
                    style: drawerButtonStyle,
                    icon: const Icon(AIcons.settings),
                    label: Text(l10n.drawerSettingsButton),
                  ),
                ],
              ),
              FutureBuilder<List<Object>>(
                future: _profileSwitchFuture,
                builder: (context, snapshot) {
                  final flags = snapshot.data;
                  if (flags == null) return const SizedBox();

                  final canRequestInteractAcrossProfiles = flags[0] as bool;
                  final canSwitchProfile = flags[1] as bool;
                  final profileSwitchingLabel = flags[2] as String;
                  if ((!canRequestInteractAcrossProfiles && !canSwitchProfile) || profileSwitchingLabel.isEmpty) return const SizedBox();

                  return OutlinedButton(
                    onPressed: () async {
                      if (canSwitchProfile) {
                        await appProfileService.switchProfile();
                      } else {
                        _profileSwitchPermissionRequested = await appProfileService.requestInteractAcrossProfiles();
                      }
                    },
                    child: Text(profileSwitchingLabel),
                  );
                },
              ),
              const SizedBox(height: 8),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildHomeLink() {
    // route name used for display purposes, no actual routing
    const displayRoute = HomePage.routeName;
    const leading = DrawerPageIcon(route: displayRoute);
    const title = DrawerPageTitle(route: displayRoute);

    switch (settings.homeNavItem.route) {
      case CollectionPage.routeName:
        final filters = settings.homeCustomCollection;
        if (filters.isNotEmpty) {
          return CollectionNavTile(
            leading: leading,
            title: title,
            filters: filters,
            isSelected: () => setEquals(currentCollection?.filters, filters),
          );
        }
      case ExplorerPage.routeName:
        final path = settings.homeCustomExplorerPath;
        if (path != null) {
          return PageNavTile(
            leading: leading,
            title: title,
            navItem: AvesNavItem(route: ExplorerPage.routeName, path: path),
            isSelected: () => widget.currentExplorerPath == path,
          );
        }
    }
    return const SizedBox();
  }

  List<Widget> _buildTypeLinks() {
    final hiddenFilters = settings.hiddenFilters;
    final typeBookmarks = settings.drawerTypeBookmarks;
    final currentFilters = currentCollection?.filters;
    final tiles = typeBookmarks
        .where((filter) => !hiddenFilters.contains(filter))
        .map(
          (filter) => CollectionNavTile(
            // key is expected by test driver
            key: Key('drawer-type-${filter?.key}'),
            leading: DrawerFilterIcon(filter: filter),
            title: DrawerFilterTitle(filter: filter),
            filters: {filter},
            isSelected: () {
              if (currentFilters == null || currentFilters.length > 1) return false;
              return currentFilters.firstOrNull == filter;
            },
          ),
        )
        .toList();
    return [
      if (tiles.isNotEmpty) const SizedBox(height: 8),
      ...tiles,
    ];
  }

  Widget _buildAlbumLinks(BuildContext context) {
    final source = context.read<CollectionSource>();
    final currentFilters = currentCollection?.filters;
    return StreamBuilder<AlbumsChangedEvent>(
      stream: source.eventBus.on<AlbumsChangedEvent>(),
      builder: (context, _) {
        final albums = AppDrawer.effectiveAlbumBookmarks(context);
        if (albums.isEmpty) return const SizedBox();
        return Column(
          children: [
            const Divider(),
            ...albums.map(
              (filter) => AlbumNavTile(
                filter: filter,
                isSelected: () {
                  if (currentFilters == null || currentFilters.length != 1) return false;
                  return currentFilters.firstOrNull == filter;
                },
              ),
            ),
          ],
        );
      },
    );
  }

  List<Widget> _buildPageLinks(BuildContext context) {
    final pageBookmarks = settings.drawerPageBookmarks;
    if (pageBookmarks.isEmpty) return [];

    final source = context.read<CollectionSource>();
    final itemCountFormatter = settings.avesLocale.decimalNumberFormat();
    return [
      const Divider(),
      ...pageBookmarks.map((route) {
        Widget? trailing;
        switch (route) {
          case AlbumListPage.routeName:
            trailing = StreamBuilder<AlbumsChangedEvent>(
              stream: source.eventBus.on<AlbumsChangedEvent>(),
              builder: (context, _) => Text(itemCountFormatter.format(source.rawAlbums.length)),
            );
          case CountryListPage.routeName:
            trailing = StreamBuilder<CountriesChangedEvent>(
              stream: source.eventBus.on<CountriesChangedEvent>(),
              builder: (context, _) => Text(itemCountFormatter.format(source.sortedCountries.length)),
            );
          case PlaceListPage.routeName:
            trailing = StreamBuilder<PlacesChangedEvent>(
              stream: source.eventBus.on<PlacesChangedEvent>(),
              builder: (context, _) => Text(itemCountFormatter.format(source.sortedPlaces.length)),
            );
          case TagListPage.routeName:
            trailing = StreamBuilder<TagsChangedEvent>(
              stream: source.eventBus.on<TagsChangedEvent>(),
              builder: (context, _) => Text(itemCountFormatter.format(source.sortedTags.length)),
            );
        }

        return PageNavTile(
          // key is expected by test driver
          key: Key('drawer-page-$route'),
          trailing: trailing,
          navItem: AvesNavItem(route: route),
        );
      }),
    ];
  }

  Widget binTile(BuildContext context) {
    final source = context.read<CollectionSource>();
    final trashSize = source.trashedEntries.map((entry) => entry.sizeBytes).sum;

    const filter = TrashFilter.instance;
    return CollectionNavTile(
      leading: const DrawerFilterIcon(filter: filter),
      title: const DrawerFilterTitle(filter: filter),
      trailing: Text(formatFileSize(settings.avesLocale, trashSize, round: 0)),
      filters: {filter},
      isSelected: () => currentCollection?.filters.contains(filter) ?? false,
    );
  }

  Widget get debugTile => const PageNavTile(
    // key is expected by test driver
    key: Key('drawer-debug'),
    navItem: AvesNavItem(route: AppDebugPage.routeName),
  );

  void _showCloudSettingsDialog(BuildContext context, CollectionSource source) async {
    final status = await cloudSyncService.refreshStatus();
    final lanUrlController = TextEditingController(
      text: settings.store.getString('cloud_lan_url') ?? (status?['lan_url'] as String?) ?? 'http://192.168.5.3:8088',
    );
    final lanTokenController = TextEditingController(
      text: settings.store.getString('cloud_lan_token') ?? (status?['lan_token'] as String?) ?? '',
    );
    final clientIdController = TextEditingController(
      text: settings.store.getString('cloud_oauth_client_id') ?? (status?['oauth_client_id'] as String?) ?? '',
    );
    final clientSecretController = TextEditingController(
      text: settings.store.getString('cloud_oauth_client_secret') ?? (status?['oauth_client_secret'] as String?) ?? '',
    );

    if (!context.mounted) return;

    await showDialog(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Row(
            children: [
              Icon(Icons.cloud_sync, color: Colors.blue),
              SizedBox(width: 8),
              Text('云端相册与 Drive 设置'),
            ],
          ),
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                TextField(
                  controller: lanUrlController,
                  decoration: const InputDecoration(
                    labelText: '局域网服务地址 (LAN URL)',
                    hintText: 'http://192.168.5.3:8088',
                    isDense: true,
                  ),
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: lanTokenController,
                  decoration: const InputDecoration(
                    labelText: '局域网 API Token (可选)',
                    hintText: '留空表示无密码',
                    isDense: true,
                  ),
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: clientIdController,
                  decoration: const InputDecoration(
                    labelText: 'Google OAuth Client ID',
                    hintText: 'xxxx.apps.googleusercontent.com',
                    isDense: true,
                  ),
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: clientSecretController,
                  decoration: const InputDecoration(
                    labelText: 'Google OAuth Client Secret (可选)',
                    hintText: '桌面客户端或免审核秘钥',
                    isDense: true,
                  ),
                ),
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(),
              child: const Text('取消'),
            ),
            OutlinedButton(
              onPressed: () async {
                final lanUrl = lanUrlController.text.trim();
                final lanToken = lanTokenController.text.trim();
                final clientId = clientIdController.text.trim();
                final clientSecret = clientSecretController.text.trim();

                await settings.store.setString('cloud_lan_url', lanUrl);
                await settings.store.setString('cloud_lan_token', lanToken);
                await settings.store.setString('cloud_oauth_client_id', clientId);
                await settings.store.setString('cloud_oauth_client_secret', clientSecret);

                await cloudSyncService.configureCloud(
                  lanBaseUrl: lanUrl,
                  lanApiToken: lanToken,
                  oauthClientId: clientId,
                  oauthClientSecret: clientSecret,
                );

                if (dialogContext.mounted) {
                  Navigator.of(dialogContext).pop();
                }

                if (context.mounted) {
                  showFeedback(context, FeedbackType.info, '正在调起 Google Drive 授权...');
                }

                final ok = await cloudSyncService.authorizeGoogleDrive(
                  clientId: clientId,
                  clientSecret: clientSecret,
                );

                if (context.mounted) {
                  showFeedback(
                    context,
                    FeedbackType.info,
                    ok ? 'Google Drive 授权成功，正在同步...' : '授权未完成或已取消',
                  );
                }
                if (ok) {
                  await cloudSyncService.sync(source);
                }
              },
              child: const Text('授权 Google Drive'),
            ),
            FilledButton(
              onPressed: () async {
                final lanUrl = lanUrlController.text.trim();
                final lanToken = lanTokenController.text.trim();
                final clientId = clientIdController.text.trim();
                final clientSecret = clientSecretController.text.trim();

                await settings.store.setString('cloud_lan_url', lanUrl);
                await settings.store.setString('cloud_lan_token', lanToken);
                await settings.store.setString('cloud_oauth_client_id', clientId);
                await settings.store.setString('cloud_oauth_client_secret', clientSecret);

                await cloudSyncService.configureCloud(
                  lanBaseUrl: lanUrl,
                  lanApiToken: lanToken,
                  oauthClientId: clientId,
                  oauthClientSecret: clientSecret,
                );

                if (dialogContext.mounted) {
                  Navigator.of(dialogContext).pop();
                }

                if (context.mounted) {
                  showFeedback(context, FeedbackType.info, '配置已保存，正在执行同步...');
                }
                await cloudSyncService.sync(source);
              },
              child: const Text('保存并同步'),
            ),
          ],
        );
      },
    );
  }
}
