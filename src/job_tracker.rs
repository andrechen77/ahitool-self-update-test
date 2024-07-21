use std::{fmt::Display, usize};

use crate::{TimeDelta, Timestamp};

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct Bucket {
    /// The number of jobs that have achieved this milestone.
    achieved: usize,
    /// The cumulative time in it took for all jobs to reach this milestone. The
    /// average time per job is this field divided by `achieved`.
    cum_achieve_time: TimeDelta,
    /// The cumulative time it took for jobs that were trying to reach this
    /// milestone but were lost to be lost. The average time per job is this
    /// field divided by the difference between the number of jobs trying to
    /// reach this field and `achieved`.
    cum_loss_time: TimeDelta,
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
    /// must be greater than 0. The loss_timestamp is the time at which the job
    /// was lost, if it was lost. If the job was not lost, which is equivalent
    /// to if the job reached the final milestone, this should be None.
    pub fn add_job(
        &mut self,
        kind: usize,
        timestamps: &[Option<Timestamp>],
        loss_timestamp: Option<Timestamp>,
    ) {
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
                bucket.cum_achieve_time = bucket.cum_achieve_time + time_till_this_milestone;
                latest_timestamp = Some(timestamp);
            }
        }

        if let Some(loss_timestamp) = loss_timestamp {
            let loss_time = if let Some(latest_timestamp) = latest_timestamp {
                loss_timestamp - latest_timestamp
            } else {
                TimeDelta::zero()
            };

            // add the time it took for the job to be lost to the next milestone
            self.bucket_after(kind, timestamps.len() - 1)
                .expect("If a job was lost, it must not have reached all milestones")
                .cum_loss_time += loss_time;
        } else {
            assert!(
                timestamps.len() == N,
                "If a job was not lost, it must have reached all milestones"
            );
        }
    }

    fn bucket_before(&self, kind: usize, milestone: usize) -> Option<&Bucket> {
        (0..milestone).rev().find_map(|ms| self.buckets[kind][ms].as_ref())
    }

    fn bucket_after(&mut self, kind: usize, milestone: usize) -> Option<&mut Bucket> {
        ((milestone + 1)..N)
            .find(|&ms| self.buckets[kind][ms].is_some())
            .and_then(|i| self.buckets[kind][i].as_mut())
    }

    /// Considering the set of all the jobs of the given kinds that have
    /// achieved the given milestone, returns three numbers.
    ///
    /// - The total number of jobs in the set.
    /// - The rate of conversion into the given set; i.e. the total number of
    /// jobs in the set divided by the total number of jobs that are either in
    /// the set or were one milestone away from reaching the set. This is 1.0 if
    /// the set is empty
    /// - The average duration it took for a job in the set to reach the
    /// specified milestone. This is zero if the set is empty.
    ///
    /// # Panics
    ///
    /// Panics if one of the specified kinds of jobs is not able to reach the
    /// specified milestone.
    pub fn calc_stats(&self, milestone: usize, kinds: &[usize]) -> CalcStatsResult {
        let buckets: Vec<&Bucket> = kinds
            .iter()
            .map(|&kind| {
                self.buckets[kind][milestone]
                    .as_ref()
                    .expect(&format!("kind {} is not able to reach milestone {}", kind, milestone))
            })
            .collect();
        let num_total = buckets.iter().map(|bucket| bucket.achieved).sum::<usize>();
        let num_potential = kinds
            .iter()
            .enumerate()
            .map(|(i, &kind)| {
                self.bucket_before(kind, milestone)
                    .map(|b| b.achieved)
                    .unwrap_or(buckets[i].achieved)
            })
            .sum::<usize>();
        let conversion_rate =
            if num_potential == 0 { 1.0 } else { num_total as f64 / num_potential as f64 };
        let total_time_to_achieve =
            buckets.iter().map(|bucket| bucket.cum_achieve_time).sum::<TimeDelta>();
        let average_time_to_achieve = if num_total == 0 {
            TimeDelta::zero()
        } else {
            total_time_to_achieve / num_total.try_into().unwrap()
        };

        CalcStatsResult { num_total, conversion_rate, average_time_to_achieve }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct CalcStatsResult {
    pub num_total: usize,
    pub conversion_rate: f64,
    pub average_time_to_achieve: TimeDelta,
}

impl<const M: usize, const N: usize> Display for JobTracker<M, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for row in self.buckets.iter() {
            for bucket in row.iter() {
                if let Some(bucket) = bucket {
                    write!(f, "({:3} {:5})", bucket.achieved, bucket.cum_achieve_time)?;
                } else {
                    write!(f, "(--- -----)")?;
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
        let tu = TimeDelta::days(10);
        let tracker = JobTracker {
            buckets: [
                [
                    Some(Bucket { achieved: 80, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 70, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 60, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 50, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 40, cum_achieve_time: tu, cum_loss_time: tu }),
                ],
                [
                    Some(Bucket { achieved: 40, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 35, cum_achieve_time: tu, cum_loss_time: tu }),
                    None,
                    Some(Bucket { achieved: 25, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 20, cum_achieve_time: tu, cum_loss_time: tu }),
                ],
                [
                    Some(Bucket { achieved: 20, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 17, cum_achieve_time: tu, cum_loss_time: tu }),
                    None,
                    Some(Bucket { achieved: 12, cum_achieve_time: tu, cum_loss_time: tu }),
                    Some(Bucket { achieved: 10, cum_achieve_time: tu, cum_loss_time: tu }),
                ],
            ],
        };

        assert_eq!(
            tracker.calc_stats(0, &[0, 1, 2]),
            CalcStatsResult {
                num_total: 80 + 40 + 20,
                conversion_rate: 1.0,
                average_time_to_achieve: tu * 3 / (80 + 40 + 20),
            }
        );
        assert_eq!(
            tracker.calc_stats(1, &[0, 1, 2]),
            CalcStatsResult {
                num_total: 70 + 35 + 17,
                conversion_rate: (70 + 35 + 17) as f64 / (80 + 40 + 20) as f64,
                average_time_to_achieve: tu * 3 / (70 + 35 + 17),
            }
        );
        assert_eq!(
            tracker.calc_stats(2, &[0]),
            CalcStatsResult {
                num_total: 60,
                conversion_rate: 60.0 / 70.0,
                average_time_to_achieve: tu / 60,
            }
        );
        assert_eq!(
            tracker.calc_stats(3, &[0, 1]),
            CalcStatsResult {
                num_total: 50 + 25,
                conversion_rate: (50 + 25) as f64 / (60 + 35) as f64,
                average_time_to_achieve: tu * 2 / (50 + 25),
            }
        );
        assert_eq!(
            tracker.calc_stats(3, &[2]),
            CalcStatsResult {
                num_total: 12,
                conversion_rate: 12.0 / 17.0,
                average_time_to_achieve: tu / 12,
            }
        );
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

        tracker.add_job(0, &[None, Some(dt(1)), Some(dt(2)), Some(dt(4)), Some(dt(8))], None);
        assert_eq!(
            tracker.buckets[0],
            [
                Some(Bucket { achieved: 1, cum_achieve_time: td(0), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 1, cum_achieve_time: td(0), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 1, cum_achieve_time: td(1), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 1, cum_achieve_time: td(2), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 1, cum_achieve_time: td(4), cum_loss_time: td(0) }),
            ]
        );

        tracker.add_job(0, &[None, Some(dt(2)), None, Some(dt(10))], Some(dt(12)));
        assert_eq!(
            tracker.buckets[0],
            [
                Some(Bucket { achieved: 2, cum_achieve_time: td(0), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 2, cum_achieve_time: td(0), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 2, cum_achieve_time: td(1), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 2, cum_achieve_time: td(10), cum_loss_time: td(0) }),
                Some(Bucket { achieved: 1, cum_achieve_time: td(4), cum_loss_time: td(2) }),
            ]
        );
    }
}
