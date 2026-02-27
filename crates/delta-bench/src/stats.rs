#[derive(Clone, Debug, PartialEq)]
pub struct SampleStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub median_ms: f64,
}

pub fn compute_stats(samples_ms: &[f64]) -> Option<SampleStats> {
    if samples_ms.is_empty() {
        return None;
    }

    let mut values = samples_ms.to_vec();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let len = values.len();
    let sum: f64 = values.iter().sum();
    let median_ms = if len.is_multiple_of(2) {
        (values[(len / 2) - 1] + values[len / 2]) / 2.0
    } else {
        values[len / 2]
    };

    Some(SampleStats {
        min_ms: *values.first().unwrap_or(&0.0),
        max_ms: *values.last().unwrap_or(&0.0),
        mean_ms: sum / (len as f64),
        median_ms,
    })
}
