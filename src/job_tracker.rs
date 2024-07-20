use std::{fmt::Display, usize};

use crate::{Timestamp, TimeDelta};

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct Bucket {
    /// The number of jobs that have achieved this milestone.
    achieved: usize,
    /// The number of jobs that were lost while at this milestone.
    lost_here: usize,
    /// The cumulative time in seconds it took for all jobs to reach this
    /// milestone. The average time per job is this field divided by `achieved`.
    total_time: TimeDelta,
}

/// Each row corresponds to one possible kind of job, and tracks data for that
/// kind of job.
#[derive(Debug)]
pub struct JobTracker<const M: usize, const N: usize> {
    buckets: [[Option<Bucket>; N]; M],
}

impl<const M: usize, const N: usize> JobTracker<M, N> {
    pub fn new(mask: [[bool; N]; M]) -> Self {
        let buckets =
            mask.map(|row| row.map(|enabled| if enabled { Some(Bucket::default()) } else { None }));
        JobTracker { buckets }
    }

    /// Adds a job of the specified kind to the tracker. The timestamps
    /// represent when the job reached each milestone. If the timestamp for
    /// reaching a certain milestone is None, then it is equivalent to the
    /// earliest possible in-order time, unless there are no known timestamps
    /// ahead, in which case it is the latest. A timestamp must be None for a
    /// milestone that does not apply to the specific job kind. The length of
    /// the timestamps slice represents the number of milestones achieved by the
    /// job; it must not exceed the total number of job milestones (i.e. N), and
    /// must be greater than
    /// 0. If `settled` is true, then the job was settled at the final available
    /// timestamp; this is counted as a loss of the job, even if the job reached
    /// the final milestone.
    pub fn add_job(&mut self, kind: usize, timestamps: &[Option<Timestamp>], settled: bool) {
        assert!(timestamps.len() > 0 && timestamps.len() <= N);

        let mut latest_timestamp = None;
        for (milestone, &timestamp) in timestamps.iter().enumerate() {
            // only iterate over those timestamps that correspond to actual
            // milestones for this kind of job.
            let Some(bucket) = &mut self.buckets[kind][milestone] else {
                assert!(timestamp.is_none(), "Timestamp must be None for a milestone that does not apply to the specific job kind");
                continue;
            };

            bucket.achieved += 1;
            if let Some(timestamp) = timestamp {
                // add the time it took to reach this milestone
                let time_till_this_milestone = if let Some(latest_timestamp) = latest_timestamp {
                    timestamp - latest_timestamp
                } else {
                    TimeDelta::zero()
                };
                bucket.total_time = bucket.total_time + time_till_this_milestone;
                latest_timestamp = Some(timestamp);
            }
        }
        if settled {
            let last_milestone_achieved = timestamps.len() - 1;
            self.buckets[kind][last_milestone_achieved].as_mut().unwrap().lost_here += 1;
        }
    }

    fn bucket_after(&self, kind: usize, milestone: usize) -> Option<&Bucket> {
        ((milestone + 1)..N).find_map(|next_col| self.buckets[kind][next_col].as_ref())
    }

    /// Given all the jobs that have achieved the given milestone, returns three
    /// numbers. The first is the number of jobs that have reached the specified
    /// milestone. The second is the number of jobs that have achieved that
    /// milestone that are no longer pending at that milestone (but might be
    /// pending at a later milestone). The third is the percentage, out of all
    /// jobs NOT pending at that milestone, that have moved on to the next
    /// milestone (i.e. not counting jobs where it is yet to be decided whether
    /// it will move on to the next milestone). If `kind` is not None, then it
    /// only considers jobs of that kind. A denominator of 0 results in the
    /// corresponding fraction being 1.0
    pub fn get_stats(&self, milestone: usize, kind: Option<usize>) -> GetStatsResult {
        let (moved_on, lost, total, moveon_time) = if let Some(kind) = kind {
            let origin_bucket = self.buckets[kind][milestone].as_ref().unwrap();
            let total = origin_bucket.achieved;
            let lost = origin_bucket.lost_here;
            let (moved_on, moveon_time) = self
                .bucket_after(kind, milestone)
                .map(|b| (b.achieved, b.total_time))
                .unwrap_or((0, TimeDelta::zero()));

            (moved_on, lost, total, moveon_time)
        } else {
            let buckets = (0..M)
                .filter_map(|kind| {
                    self.buckets[kind][milestone].as_ref().map(|bucket| (kind, bucket))
                })
                .collect::<Vec<_>>();
            let total = buckets.iter().map(|(_, bucket)| bucket.achieved).sum::<usize>();
            let lost = buckets.iter().map(|(_, bucket)| bucket.lost_here).sum::<usize>();
            let (moved_on, moveon_time) = buckets
                .iter()
                .filter_map(|&(kind, _)| {
                    self.bucket_after(kind, milestone).map(|b| (b.achieved, b.total_time))
                })
                .fold((0, TimeDelta::zero()), |(acc_moved_on, acc_time), (moved_on, time)| {
                    (acc_moved_on + moved_on, acc_time + time)
                });

            (moved_on, lost, total, moveon_time)
        };
        let non_pending = moved_on + lost;
        GetStatsResult {
            total,
            non_pending,
            conversion_rate: if non_pending == 0 {
                1.0
            } else {
                moved_on as f64 / non_pending as f64
            },
            average_time_to_move_on: if moved_on == 0 {
                TimeDelta::zero()
            } else {
                moveon_time / moved_on.try_into().unwrap()
            },
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct GetStatsResult {
    pub total: usize,
    pub non_pending: usize,
    pub conversion_rate: f64,
    pub average_time_to_move_on: TimeDelta,
}

impl<const M: usize, const N: usize> Display for JobTracker<M, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for row in self.buckets.iter() {
            for bucket in row.iter() {
                if let Some(bucket) = bucket {
                    write!(
                        f,
                        "({:3} {:3} {:5})",
                        bucket.achieved, bucket.lost_here, bucket.total_time
                    )?;
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
        let time_unit = TimeDelta::days(10);
        let tracker = JobTracker {
            buckets: [
                [
                    Some(Bucket { achieved: 80, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 70, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 60, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 50, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 40, lost_here: 1, total_time: time_unit }),
                ],
                [
                    Some(Bucket { achieved: 40, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 35, lost_here: 1, total_time: time_unit }),
                    None,
                    Some(Bucket { achieved: 25, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 20, lost_here: 1, total_time: time_unit }),
                ],
                [
                    Some(Bucket { achieved: 20, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 17, lost_here: 1, total_time: time_unit }),
                    None,
                    Some(Bucket { achieved: 12, lost_here: 1, total_time: time_unit }),
                    Some(Bucket { achieved: 10, lost_here: 1, total_time: time_unit }),
                ],
            ],
        };

        assert_eq!(
            tracker.get_stats(0, None),
            GetStatsResult {
                total: 80 + 40 + 20,
                non_pending: 71 + 36 + 18,
                conversion_rate: (70 + 35 + 17) as f64 / (71 + 36 + 18) as f64,
                average_time_to_move_on: time_unit * 3 / (70 + 35 + 17),
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(0)),
            GetStatsResult {
                total: 70,
                non_pending: 61,
                conversion_rate: 60.0 / 61.0,
                average_time_to_move_on: time_unit / 60,
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(1)),
            GetStatsResult {
                total: 35,
                non_pending: 26,
                conversion_rate: 25.0 / 26.0,
                average_time_to_move_on: time_unit / 25,
            }
        );
        assert_eq!(
            tracker.get_stats(1, Some(2)),
            GetStatsResult {
                total: 17,
                non_pending: 13,
                conversion_rate: 12.0 / 13.0,
                average_time_to_move_on: time_unit / 12,
            }
        );
        assert_eq!(
            tracker.get_stats(2, None),
            GetStatsResult {
                total: 60,
                non_pending: 51,
                conversion_rate: 50.0 / 51.0,
                average_time_to_move_on: time_unit / 50,
            }
        )
    }

    #[test]
    fn add_jobs() {
        // date-time
        fn dt(seconds: i64) -> Timestamp {
            Timestamp::from_timestamp(seconds, 0).unwrap()
        }
        // time-delta
        fn td(seconds: i64) -> TimeDelta {
            TimeDelta::seconds(seconds)
        }

        let mut tracker = JobTracker::new([
            [true, true, true, true, true],
            [true, true, false, true, true],
            [true, true, false, true, true],
        ]);

        tracker.add_job(0, &[None, Some(dt(1)), Some(dt(2)), Some(dt(4)), Some(dt(8))], false);
        assert_eq!(
            tracker.buckets[0],
            [
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(0) }),
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(0) }),
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(1) }),
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(2) }),
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(4) }),
            ]
        );

        tracker.add_job(0, &[None, Some(dt(2)), None, Some(dt(10))], true);
        assert_eq!(
            tracker.buckets[0],
            [
                Some(Bucket { achieved: 2, lost_here: 0, total_time: td(0) }),
                Some(Bucket { achieved: 2, lost_here: 0, total_time: td(0) }),
                Some(Bucket { achieved: 2, lost_here: 0, total_time: td(1) }),
                Some(Bucket { achieved: 2, lost_here: 1, total_time: td(10) }),
                Some(Bucket { achieved: 1, lost_here: 0, total_time: td(4) }),
            ]
        );
    }
}
