use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, TimeUnit};
use rustler::Term;

pub fn get_array_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    // https://docs.datalayers.cn/datalayers/latest/sql-reference/data-type.html
    match data_type {
        DataType::Int8 => Box::new(Int8Builder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),

        DataType::UInt8 => Box::new(UInt8Builder::new()),
        DataType::UInt16 => Box::new(UInt16Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::UInt64 => Box::new(UInt64Builder::new()),

        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),

        DataType::Timestamp(TimeUnit::Second, Some(zone)) => {
            Box::new(TimestampSecondBuilder::new().with_timezone(zone.clone()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(zone)) => {
            Box::new(TimestampMillisecondBuilder::new().with_timezone(zone.clone()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => {
            Box::new(TimestampMicrosecondBuilder::new().with_timezone(zone.clone()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) => {
            Box::new(TimestampNanosecondBuilder::new().with_timezone(zone.clone()))
        }

        DataType::Boolean => Box::new(BooleanBuilder::new()),

        DataType::Utf8 => Box::new(StringBuilder::new()),

        unimplemented => {
            eprintln!("{unimplemented}");
            unimplemented!()
        }
    }
}

pub fn append_value_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    term: Term,
    data_type: &DataType,
) {
    match data_type {
        DataType::Int8 => {
            let val = term.decode::<i8>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Int16 => {
            let val = term.decode::<i16>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Int32 => {
            let val = term.decode::<i32>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Int64 => {
            let val = term.decode::<i64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(val)
        }

        DataType::UInt8 => {
            let val = term.decode::<u8>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::UInt16 => {
            let val = term.decode::<u16>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::UInt32 => {
            let val = term.decode::<u32>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::UInt64 => {
            let val = term.decode::<u64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap()
                .append_value(val)
        }

        DataType::Float32 => {
            let val = term.decode::<f32>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Float64 => {
            let val = term.decode::<f64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_value(val)
        }

        DataType::Timestamp(TimeUnit::Second, _) => {
            let val = term.decode::<i64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<TimestampSecondBuilder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let val = term.decode::<i64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMillisecondBuilder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let val = term.decode::<i64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(val)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let val = term.decode::<i64>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<TimestampNanosecondBuilder>()
                .unwrap()
                .append_value(val)
        }

        DataType::Boolean => {
            let val = term.decode::<bool>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_value(val)
        }

        DataType::Utf8 => {
            let val = term.decode::<String>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(val)
        }

        unimplemented => {
            eprintln!("{unimplemented}");
            unimplemented!()
        }
    };
}
