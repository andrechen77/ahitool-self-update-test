use std::{fmt::Display, usize};

type Timestamp = i64;

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct Bucket {
    /// The number of jobs that have achieved this stage.
    achieved: usize,
    /// The number of jobs that were lost while at this stage.
    lost_here: usize,
    /// The cumulative time in seconds it took for all jobs to reach this stage.
    /// The average time per job is this field divided by `achieved`.
    total_time: i64,
}

/// Each row corresponds to one possible kind of job, and tracks data for that
/// kind of job.
#[derive(Debug)]
pub struct JobTracker<const M: usize, const N: usize> {
    buckets: [[Option<Bucket>; N]; M],
}

impl<const M: usize, const N: usize> JobTracker<M, N> {
    pub fn new(mask: [[bool; N]; M]) -> Self {
        let buckets = mask.map(|row| row.map(|enabled| if enabled { Some(Bucket::default()) } else { None }));
        JobTracker { buckets }
    }

	/// Adds a job of the specified kind to the tracker. The timestamps
	/// represent when the job reached each stage. If the timestamp for reaching
	/// a certain stage is None, then it is equivalent to the earliest possible
	/// in-order time. A timestamp must be None for a stage that does
	/// not apply to the specific job kind. The length of the timestamps slice
	/// represents the number of stages achieved by the job; it must not exceed
	/// the total number of job stages (i.e. N), and must be greater than
	/// 0. If `lost` is true, then the job was lost at the final available
	/// timestamp.
    pub fn add_job(&mut self, kind: usize, timestamps: &[Option<Timestamp>], lost: bool) {
		assert!(timestamps.len() > 0 && timestamps.len() <= N);

        let mut latest_timestamp = None;
        for (stage, &timestamp) in timestamps.iter().enumerate() {
            // only iterate over those timestamps that correspond to actual
            // stages for this kind of job.
            let Some(bucket) = &mut self.buckets[kind][stage] else {
                assert!(timestamp.is_none(), "Timestamp must be None for a stage that does not apply to the specific job kind");
                continue;
            };

            bucket.achieved += 1;
            if let Some(timestamp) = timestamp {
                // add the time it took to reach this stage
                let time_till_this_stage = if let Some(latest_timestamp) = latest_timestamp {
                    timestamp - latest_timestamp
                } else {
                    Timestamp::MAX
                };
                bucket.total_time = bucket.total_time.saturating_add(time_till_this_stage);
                latest_timestamp = Some(timestamp);
            }
        }
        if lost {
			let last_stage_achieved = timestamps.len() - 1;
            self.buckets[kind][last_stage_achieved].as_mut().unwrap().lost_here += 1;
        }
    }

    fn bucket_after(&self, kind: usize, stage: usize) -> Option<&Bucket> {
        ((stage + 1)..N).find_map(|next_col| self.buckets[kind][next_col].as_ref())
    }

    /// Given all the jobs that have achieved the given stage, returns three
    /// numbers. The first is the number of jobs that have reached the specified
    /// stage. The second is the number of jobs that have achieved that stage
    /// that are no longer pending at that stage (but might be pending at a
    /// later stage). The third is the percentage, out of all jobs NOT pending
    /// at that stage, that have moved on to the next stage (i.e. not counting
	/// jobs where it is yet to be decided whether it will move on to the next
	/// stage). If `kind` is not None, then it only considers jobs of that kind.
	/// A denominator of 0 results in the corresponding fraction being 1.0
    pub fn get_stats(&self, stage: usize, kind: Option<usize>) -> GetStatsResult {
        let (moved_on, lost, total, moveon_time) = if let Some(kind) = kind {
            let origin_bucket = self.buckets[kind][stage].as_ref().unwrap();
            let total = origin_bucket.achieved;
            let lost = origin_bucket.lost_here;
            let (moved_on, moveon_time) = self
                .bucket_after(kind, stage)
                .map(|b| (b.achieved, b.total_time as f64))
                .unwrap_or((0, 0.0));

            (moved_on, lost, total, moveon_time)

        } else {
            let buckets = (0..M).filter_map(|kind| {
                self.buckets[kind][stage].as_ref().map(|bucket| (kind, bucket))
            }).collect::<Vec<_>>();
            let total = buckets.iter().map(|(_, bucket)| bucket.achieved).sum::<usize>();
            let lost = buckets.iter().map(|(_, bucket)| bucket.lost_here).sum::<usize>();
            let (moved_on, moveon_time) = buckets
                .iter()
                .filter_map(|&(kind, _)| {
                    self.bucket_after(kind, stage)
                        .map(|b| (b.achieved, b.total_time as f64))
                })
                .fold((0, 0.0), |(acc_moved_on, acc_time), (moved_on, time)| (acc_moved_on + moved_on, acc_time + time));

            (moved_on, lost, total, moveon_time)
        };
        let non_pending = moved_on + lost;
        GetStatsResult {
            total,
            non_pending,
            conversion_rate: if non_pending == 0 { 1.0 } else { moved_on as f64 / non_pending as f64 },
            average_time_to_move_on: if moved_on == 0 { 0.0 } else { moveon_time / moved_on as f64 },
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct GetStatsResult {
    pub total: usize,
    pub non_pending: usize,
    pub conversion_rate: f64,
    pub average_time_to_move_on: f64, // in seconds
}

impl<const M: usize, const N: usize> Display for JobTracker<M, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for row in self.buckets.iter() {
            for bucket in row.iter() {
                if let Some(bucket) = bucket {
                    write!(f, "({:3} {:3} {:5})", bucket.achieved, bucket.lost_here, bucket.total_time)?;
                } else {
                    write!(f, "(--- --- -----)")?;
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
    fn get_stats() {
        let tracker = JobTracker { buckets: [
            [
                Some(Bucket { achieved: 80, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 70, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 60, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 50, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 40, lost_here: 1, total_time: 1000 }),
            ],
            [
                Some(Bucket { achieved: 40, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 35, lost_here: 1, total_time: 1000 }),
                None,
                Some(Bucket { achieved: 25, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 20, lost_here: 1, total_time: 1000 }),
            ],
            [
                Some(Bucket { achieved: 20, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 17, lost_here: 1, total_time: 1000 }),
                None,
                Some(Bucket { achieved: 12, lost_here: 1, total_time: 1000 }),
                Some(Bucket { achieved: 10, lost_here: 1, total_time: 1000 }),
            ],
        ] };

        assert_eq!(
            tracker.get_stats(0, None),
            GetStatsResult {
                total: 80 + 40 + 20,
                non_pending: 71 + 36 + 18,
                conversion_rate: (70 + 35 + 17) as f64 / (71 + 36 + 18) as f64,
                average_time_to_move_on: (1000 + 1000 + 1000) as f64 / (70 + 35 + 17) as f64,
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(0)),
            GetStatsResult {
                total: 70,
                non_pending: 61,
                conversion_rate: 60.0 / 61.0,
                average_time_to_move_on: 1000.0 / 60.0,
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(1)),
            GetStatsResult {
                total: 35,
                non_pending: 26,
                conversion_rate: 25.0 / 26.0,
                average_time_to_move_on: 1000.0 / 25.0,
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(2)),
            GetStatsResult {
                total: 17,
                non_pending: 13,
                conversion_rate: 12.0 / 13.0,
                average_time_to_move_on: 1000.0 / 12.0,
            }
        );
        assert_eq!(
            tracker.get_stats(2, None),
            GetStatsResult {
                total: 60,
                non_pending: 51,
                conversion_rate: 50.0 / 51.0,
                average_time_to_move_on: 1000.0 / 50.0,
            }
        )
    }

    #[test]
    fn add_jobs() {
        let mut tracker = JobTracker::new(
            [
                [true, true, true, true, true],
                [true, true, false, true, true],
                [true, true, false, true, true],
            ]
        );

        tracker.add_job(0, &[None, Some(1), Some(2), Some(4), Some(8)], false);
        assert_eq!(tracker.buckets[0], [
            Some(Bucket { achieved: 1, lost_here: 0, total_time: 0 }),
            Some(Bucket { achieved: 1, lost_here: 0, total_time: i64::MAX }),
            Some(Bucket { achieved: 1, lost_here: 0, total_time: 1 }),
            Some(Bucket { achieved: 1, lost_here: 0, total_time: 2 }),
            Some(Bucket { achieved: 1, lost_here: 0, total_time: 4 }),
        ]);

        tracker.add_job(0, &[None, Some(2), None, Some(10)], true);
        assert_eq!(tracker.buckets[0], [
            Some(Bucket { achieved: 2, lost_here: 0, total_time: 0 }),
            Some(Bucket { achieved: 2, lost_here: 0, total_time: i64::MAX }),
            Some(Bucket { achieved: 2, lost_here: 0, total_time: 1 }),
            Some(Bucket { achieved: 2, lost_here: 1, total_time: 10 }),
            Some(Bucket { achieved: 1, lost_here: 0, total_time: 4 }),
        ]);
    }
}
