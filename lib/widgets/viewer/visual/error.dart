import 'dart:io';

import 'package:aves/model/entry/entry.dart';
import 'package:aves/model/entry/extensions/props.dart';
import 'package:aves/model/entry/origins.dart';
import 'package:aves/theme/icons.dart';
import 'package:aves/widgets/common/extensions/build_context.dart';
import 'package:aves/widgets/common/identity/empty.dart';
import 'package:aves/widgets/viewer/entry_viewer_page.dart';
import 'package:flutter/foundation.dart';
import 'package:material_ui/material_ui.dart';

class ErrorView extends StatefulWidget {
  final AvesEntry entry;
  final VoidCallback onTap;
  final Object? error;

  const new({
    super.key,
    required this.entry,
    required this.onTap,
    this.error,
  });

  @override
  State<ErrorView> createState() => _ErrorViewState();
}

class _ErrorViewState extends State<ErrorView> {
  late Future<bool> _exists;

  AvesEntry get entry => widget.entry;

  @override
  void initState() {
    super.initState();
    final path = entry.storagePath;
    _exists = (entry.origin == EntryOrigins.cloud || path == null) ? SynchronousFuture(true) : File(path).exists();
  }

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () => widget.onTap(),
      // use container to expand constraints, so that the user can tap anywhere
      child: Container(
        // opaque to cover potential lower quality layer below
        color: EntryViewerPage.getBackground(context),
        child: FutureBuilder<bool>(
          future: _exists,
          builder: (context, snapshot) {
            if (snapshot.connectionState != ConnectionState.done) return const SizedBox();
            final exists = snapshot.data!;
            return EmptyContent(
              icon: exists ? AIcons.error : AIcons.broken,
              text: exists ? context.l10n.viewerErrorUnknown : context.l10n.viewerErrorDoesNotExist,
              bottom: (widget.error != null)
                  ? Padding(
                      padding: const EdgeInsets.only(top: 12, left: 24, right: 24),
                      child: Text(
                        widget.error.toString(),
                        textAlign: TextAlign.center,
                        maxLines: 4,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          fontSize: 12,
                          color: Colors.grey,
                        ),
                      ),
                    )
                  : null,
              alignment: Alignment.center,
              safeBottom: false,
            );
          },
        ),
      ),
    );
  }
}
