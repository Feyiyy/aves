import 'package:aves/model/covers.dart';
import 'package:aves/model/filters/container/album_group.dart';
import 'package:aves/model/filters/covered/covered.dart';
import 'package:aves/model/filters/filters.dart';
import 'package:aves/theme/colors.dart';
import 'package:aves/theme/icons.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:provider/provider.dart';

class HiddenAlbumFilter extends CollectionFilter with CoveredFilter, AlbumBaseFilter {
  static const type = 'hidden_album';

  static const instance = HiddenAlbumFilter._private();
  static const instanceReversed = HiddenAlbumFilter._private(reversed: true);

  const new _private({super.reversed = false});

  factory fromMap(Map<String, Object?> json) {
    final reversed = json['reversed'] as bool? ?? false;
    return reversed ? instanceReversed : instance;
  }

  @override
  List<Object?> get props => [reversed];

  @override
  Map<String, Object?> toJsonMap() => {
    'type': type,
    if (reversed) 'reversed': reversed,
  };

  @override
  EntryPredicate get positiveTest => (entry) => true;

  @override
  bool get exclusiveProp => false;

  @override
  String get universalLabel => 'Hidden';

  @override
  String getLabel(BuildContext context) => '已隐藏';

  @override
  Widget? iconBuilder(BuildContext context, double size, {bool allowGenericIcon = true}) => Icon(AIcons.hide, size: size);

  @override
  String get category => type;

  @override
  String get key => '$type-$reversed';

  @override
  Future<Color> color(BuildContext context) {
    final customColor = covers.of(this)?.color;
    if (customColor != null) return SynchronousFuture(customColor);
    final colors = context.read<AvesColorsData>();
    return SynchronousFuture(colors.albumScreenshots);
  }
}
