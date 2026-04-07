use deltalake_core::kernel::Snapshot;
use deltalake_core::DeltaTable;
use serde::ser::{Error as _, Impossible, SerializeSeq, SerializeTuple};
use serde::Serialize;
use serde_json::Value;

use crate::error::{BenchError, BenchResult};

pub(crate) fn clone_plain_snapshot_from_loaded_table(table: &DeltaTable) -> BenchResult<Snapshot> {
    let eager = table
        .snapshot()
        .map(|state| state.snapshot())
        .map_err(BenchError::from)?;
    let value = eager
        .serialize(FirstSequenceElementSerializer)
        .map_err(|error| BenchError::InvalidArgument(error.to_string()))?;
    Ok(serde_json::from_value(value)?)
}

// EagerSnapshot serializes the plain Snapshot as its first sequence element.
// Capture only that element so replay probes stay on the snapshot-owned
// provider path instead of falling back to the loaded eager state.
struct FirstSequenceElementSerializer;

impl serde::Serializer for FirstSequenceElementSerializer {
    type Ok = Value;
    type Error = CaptureError;
    type SerializeSeq = FirstSequenceElementCapture;
    type SerializeTuple = FirstSequenceElementCapture;
    type SerializeTupleStruct = Impossible<Value, CaptureError>;
    type SerializeTupleVariant = Impossible<Value, CaptureError>;
    type SerializeMap = Impossible<Value, CaptureError>;
    type SerializeStruct = Impossible<Value, CaptureError>;
    type SerializeStructVariant = Impossible<Value, CaptureError>;

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(FirstSequenceElementCapture { first: None })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(FirstSequenceElementCapture { first: None })
    }

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(CaptureError::custom("expected a sequence"))
    }
}

struct FirstSequenceElementCapture {
    first: Option<Value>,
}

impl SerializeSeq for FirstSequenceElementCapture {
    type Ok = Value;
    type Error = CaptureError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        if self.first.is_none() {
            self.first = Some(serde_json::to_value(value)?);
        }
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.first
            .ok_or_else(|| CaptureError::custom("sequence did not contain any elements"))
    }
}

impl SerializeTuple for FirstSequenceElementCapture {
    type Ok = Value;
    type Error = CaptureError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        SerializeSeq::end(self)
    }
}

#[derive(Debug)]
struct CaptureError(String);

impl std::fmt::Display for CaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CaptureError {}

impl serde::ser::Error for CaptureError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl From<serde_json::Error> for CaptureError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}
