use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, TimeUnit};
use rustler::{Error, Term};

macro_rules! define_type_handling_functions {
    (
        standard_types: [ $( ($dt:ident, $builder:ident, $rust_ty:ty) ),* $(,)? ],
        timestamp_types: [ $( ($unit:ident, $ts_builder:ident) ),* $(,)? ]
    ) => {
        pub fn get_array_builder(data_type: &DataType) -> Result<Box<dyn ArrayBuilder>, Error> {
            match data_type {
                $(
                    DataType::$dt => Ok(Box::new($builder::new())),
                )*
                $(
                    DataType::Timestamp(TimeUnit::$unit, Some(zone)) => Ok(Box::new(
                        $ts_builder::new().with_timezone(zone.clone()),
                    )),
                )*
                unimplemented => Err(Error::Term(Box::new(format!(
                    "unsupported type: {unimplemented}"
                )))),
            }
        }

        pub fn append_value_to_builder(
            builder: &mut Box<dyn ArrayBuilder>,
            term: Term,
            data_type: &DataType,
        ) -> Result<(), Error> {
            match data_type {
                $(
                    DataType::$dt => {
                        let val = term.decode::<$rust_ty>()?;
                        builder
                            .as_any_mut()
                            .downcast_mut::<$builder>()
                            .ok_or(Error::BadArg)?
                            .append_value(val);
                    }
                )*
                $(
                    DataType::Timestamp(TimeUnit::$unit, _) => {
                        let val = term.decode::<i64>()?;
                        builder
                            .as_any_mut()
                            .downcast_mut::<$ts_builder>()
                            .ok_or(Error::BadArg)?
                            .append_value(val);
                    }
                )*
                unimplemented => {
                    return Err(Error::Term(Box::new(format!(
                        "unsupported type: {unimplemented}"
                    ))));
                }
            };
            Ok(())
        }
    };
}

// https://docs.datalayers.cn/datalayers/latest/sql-reference/data-type.html
define_type_handling_functions! {
    standard_types: [
        (Int8, Int8Builder, i8),
        (Int16, Int16Builder, i16),
        (Int32, Int32Builder, i32),
        (Int64, Int64Builder, i64),
        (UInt8, UInt8Builder, u8),
        (UInt16, UInt16Builder, u16),
        (UInt32, UInt32Builder, u32),
        (UInt64, UInt64Builder, u64),
        (Float32, Float32Builder, f32),
        (Float64, Float64Builder, f64),
        (Boolean, BooleanBuilder, bool),
        (Utf8, StringBuilder, String),
    ],
    timestamp_types: [
        (Second, TimestampSecondBuilder),
        (Millisecond, TimestampMillisecondBuilder),
        (Microsecond, TimestampMicrosecondBuilder),
        (Nanosecond, TimestampNanosecondBuilder),
    ]
}
