use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::datatypes::*;
#[cfg(feature = "datafusion")]
use datafusion::arrow::datatypes::*;
use geo_postgis::ToPostgis;
use geo_traits::to_geo::{
    ToGeoGeometry, ToGeoGeometryCollection, ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint,
    ToGeoMultiPolygon, ToGeoPoint, ToGeoPolygon, ToGeoRect,
};
use geoarrow::array::{AsGeoArrowArray, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use pgwire::api::results::FieldInfo;
use pgwire::error::{PgWireError, PgWireResult};

use crate::encoder::Encoder;

macro_rules! encode_geo_fn {
    (
        $name:ident,
        $array_type:ty,
        $postgis_type:ty,
        $($conversion:tt)+
    ) => {
        fn $name<T: Encoder>(
            encoder: &mut T,
            array: &$array_type,
            idx: usize,
            pg_field: &FieldInfo,
        ) -> PgWireResult<()> {
            if array.is_null(idx) {
                return encoder.encode_field(&None::<$postgis_type>, pg_field);
            }

            let value = array
                .value(idx)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let converted_value = value $($conversion)+;

            encoder.encode_field(&converted_value, pg_field)
        }
    };
}

encode_geo_fn!(encode_point, geoarrow::array::PointArray, postgis::ewkb::Point,
    .to_point().to_postgis_with_srid(None));

encode_geo_fn!(encode_linestring, geoarrow::array::LineStringArray, postgis::ewkb::LineString,
    .to_line_string().to_postgis_with_srid(None));

encode_geo_fn!(encode_polygon, geoarrow::array::PolygonArray, postgis::ewkb::Polygon,
    .to_polygon().to_postgis_with_srid(None));

encode_geo_fn!(encode_multipoint, geoarrow::array::MultiPointArray, postgis::ewkb::MultiPoint,
    .to_multi_point().to_postgis_with_srid(None));

encode_geo_fn!(encode_multilinestring, geoarrow::array::MultiLineStringArray, postgis::ewkb::MultiLineString,
    .to_multi_line_string().to_postgis_with_srid(None));

encode_geo_fn!(encode_multipolygon, geoarrow::array::MultiPolygonArray, postgis::ewkb::MultiPolygon,
    .to_multi_polygon().to_postgis_with_srid(None));

encode_geo_fn!(encode_geometrycollection, geoarrow::array::GeometryCollectionArray, postgis::ewkb::GeometryCollection,
    .to_geometry_collection().to_postgis_with_srid(None));

encode_geo_fn!(encode_rect, geoarrow::array::RectArray, postgis::ewkb::Polygon,
    .to_rect().to_polygon().to_postgis_with_srid(None));

encode_geo_fn!(encode_wkt, geoarrow::array::WktArray, String,
    .to_string());

encode_geo_fn!(encode_large_wkt, geoarrow::array::LargeWktArray, String,
    .to_string());

encode_geo_fn!(encode_wkt_view, geoarrow::array::WktViewArray, String,
    .to_string());

encode_geo_fn!(encode_wkb, geoarrow::array::WkbArray, Vec<u8>,
    .buf().to_vec());

encode_geo_fn!(encode_large_wkb, geoarrow::array::LargeWkbArray, Vec<u8>,
    .buf().to_vec());

encode_geo_fn!(encode_wkb_view, geoarrow::array::WkbViewArray, Vec<u8>,
    .buf().to_vec());

encode_geo_fn!(encode_geometry, geoarrow::array::GeometryArray, postgis::ewkb::Geometry,
    .to_geometry().to_postgis_with_srid(None));

pub fn encode_geo<T: Encoder>(
    encoder: &mut T,
    geoarrow_type: GeoArrowType,
    arr: &Arc<dyn geoarrow::array::GeoArrowArray>,
    idx: usize,
    _arrow_field: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    match geoarrow_type {
        GeoArrowType::Point(_) => {
            let array = arr.as_point();
            encode_point(encoder, array, idx, pg_field)
        }
        GeoArrowType::LineString(_) => {
            let array = arr.as_line_string();
            encode_linestring(encoder, array, idx, pg_field)
        }
        GeoArrowType::Polygon(_) => {
            let array = arr.as_polygon();
            encode_polygon(encoder, array, idx, pg_field)
        }
        GeoArrowType::MultiPoint(_) => {
            let array = arr.as_multi_point();
            encode_multipoint(encoder, array, idx, pg_field)
        }
        GeoArrowType::MultiLineString(_) => {
            let array = arr.as_multi_line_string();
            encode_multilinestring(encoder, array, idx, pg_field)
        }
        GeoArrowType::MultiPolygon(_) => {
            let array = arr.as_multi_polygon();
            encode_multipolygon(encoder, array, idx, pg_field)
        }
        GeoArrowType::GeometryCollection(_) => {
            let array = arr.as_geometry_collection();
            encode_geometrycollection(encoder, array, idx, pg_field)
        }
        GeoArrowType::Rect(_) => {
            let array = arr.as_rect();
            encode_rect(encoder, array, idx, pg_field)
        }
        GeoArrowType::Wkt(_) => {
            let array = arr.as_wkt();
            encode_wkt(encoder, array, idx, pg_field)
        }
        GeoArrowType::WktView(_) => {
            let array = arr.as_wkt_view();
            encode_wkt_view(encoder, array, idx, pg_field)
        }
        GeoArrowType::LargeWkt(_) => {
            let array = arr.as_wkt();
            encode_large_wkt(encoder, array, idx, pg_field)
        }
        GeoArrowType::Wkb(_) => {
            let array = arr.as_wkb();
            encode_wkb(encoder, array, idx, pg_field)
        }
        GeoArrowType::WkbView(_) => {
            let array = arr.as_wkb_view();
            encode_wkb_view(encoder, array, idx, pg_field)
        }
        GeoArrowType::LargeWkb(_) => {
            let array = arr.as_wkb();
            encode_large_wkb(encoder, array, idx, pg_field)
        }
        GeoArrowType::Geometry(_) => {
            let array = arr.as_geometry();
            encode_geometry(encoder, array, idx, pg_field)
        }
    }
}
