use std::fmt::Display;

use crate::error::{BenchError, BenchResult};

pub(crate) fn snapshot_version_arg<T>(version: u64) -> BenchResult<T>
where
    T: TryFrom<u64>,
{
    version.try_into().map_err(|_| {
        BenchError::InvalidArgument(format!("snapshot version {version} is out of range"))
    })
}

pub(crate) fn optional_snapshot_version_arg<T>(version: Option<u64>) -> BenchResult<Option<T>>
where
    T: TryFrom<u64>,
{
    version.map(snapshot_version_arg).transpose()
}

pub(crate) fn table_version_to_u64<T>(version: T) -> BenchResult<u64>
where
    u64: TryFrom<T>,
    T: Copy + Display,
{
    u64::try_from(version).map_err(|_| {
        BenchError::InvalidArgument(format!("snapshot version {version} must be non-negative"))
    })
}

pub(crate) fn optional_table_version_to_u64<T>(version: Option<T>) -> BenchResult<Option<u64>>
where
    u64: TryFrom<T>,
    T: Copy + Display,
{
    version.map(table_version_to_u64).transpose()
}

#[cfg(test)]
mod tests {
    use super::{
        optional_snapshot_version_arg, optional_table_version_to_u64, snapshot_version_arg,
        table_version_to_u64,
    };
    use crate::error::BenchError;

    #[test]
    fn snapshot_version_arg_accepts_signed_targets() {
        let version: i64 = snapshot_version_arg(7).expect("signed target");
        assert_eq!(version, 7);
    }

    #[test]
    fn snapshot_version_arg_accepts_unsigned_targets() {
        let version: u64 = snapshot_version_arg(7).expect("unsigned target");
        assert_eq!(version, 7);
        assert_eq!(
            optional_snapshot_version_arg::<u64>(Some(9)).expect("optional target"),
            Some(9)
        );
    }

    #[test]
    fn snapshot_version_arg_rejects_out_of_range_targets() {
        let err = snapshot_version_arg::<i8>(128).expect_err("out of range");
        assert!(
            matches!(err, BenchError::InvalidArgument(message) if message.contains("out of range"))
        );
    }

    #[test]
    fn table_version_to_u64_accepts_signed_and_unsigned_versions() {
        assert_eq!(table_version_to_u64(11_i64).expect("signed version"), 11);
        assert_eq!(
            optional_table_version_to_u64(Some(13_u64)).expect("unsigned version"),
            Some(13)
        );
    }

    #[test]
    fn table_version_to_u64_rejects_negative_versions() {
        let err = table_version_to_u64(-1_i64).expect_err("negative version");
        assert!(
            matches!(err, BenchError::InvalidArgument(message) if message.contains("non-negative"))
        );
    }
}
