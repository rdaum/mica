use crate::tuple::Tuple;
use std::slice;

#[derive(Clone, Debug)]
pub(super) enum TupleBucket {
    Empty,
    One(Tuple),
    Many(Vec<Tuple>),
}

impl TupleBucket {
    pub(super) fn one(tuple: Tuple) -> Self {
        Self::One(tuple)
    }

    pub(super) fn from_sorted_unique(tuples: impl IntoIterator<Item = Tuple>) -> Self {
        let mut tuples = tuples.into_iter();
        let Some(first) = tuples.next() else {
            return Self::Empty;
        };
        let Some(second) = tuples.next() else {
            return Self::One(first);
        };

        let mut rows = Vec::with_capacity(2 + tuples.size_hint().0);
        rows.push(first);
        rows.push(second);
        rows.extend(tuples);
        Self::Many(rows)
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Many(tuples) => tuples.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub(super) fn insert(&mut self, tuple: Tuple) -> bool {
        match self {
            Self::Empty => {
                *self = Self::One(tuple);
                true
            }
            Self::One(existing) if *existing == tuple => false,
            Self::One(existing) => {
                let tuples = if tuple < *existing {
                    vec![tuple, existing.clone()]
                } else {
                    vec![existing.clone(), tuple]
                };
                *self = Self::Many(tuples);
                true
            }
            Self::Many(tuples) => {
                if let Some(last) = tuples.last() {
                    if *last == tuple {
                        return false;
                    }
                    if *last < tuple {
                        tuples.push(tuple);
                        return true;
                    }
                }

                match tuples.binary_search(&tuple) {
                    Ok(_) => false,
                    Err(index) => {
                        tuples.insert(index, tuple);
                        true
                    }
                }
            }
        }
    }

    pub(super) fn remove(&mut self, tuple: &Tuple) -> bool {
        match self {
            Self::Empty => false,
            Self::One(existing) if existing == tuple => {
                *self = Self::Empty;
                true
            }
            Self::One(_) => false,
            Self::Many(tuples) => {
                let Ok(index) = tuples.binary_search(tuple) else {
                    return false;
                };
                tuples.remove(index);
                match tuples.len() {
                    0 => *self = Self::Empty,
                    1 => *self = Self::One(tuples.pop().expect("one tuple remains")),
                    _ => {}
                }
                true
            }
        }
    }

    pub(super) fn iter(&self) -> TupleBucketIter<'_> {
        match self {
            Self::Empty => TupleBucketIter::Empty,
            Self::One(tuple) => TupleBucketIter::One(Some(tuple)),
            Self::Many(tuples) => TupleBucketIter::Many(tuples.iter()),
        }
    }

    pub(super) fn first(&self) -> Option<&Tuple> {
        match self {
            Self::Empty => None,
            Self::One(tuple) => Some(tuple),
            Self::Many(tuples) => tuples.first(),
        }
    }
}

impl<'a> IntoIterator for &'a TupleBucket {
    type Item = &'a Tuple;
    type IntoIter = TupleBucketIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(super) enum TupleBucketIter<'a> {
    Empty,
    One(Option<&'a Tuple>),
    Many(slice::Iter<'a, Tuple>),
}

impl<'a> Iterator for TupleBucketIter<'a> {
    type Item = &'a Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::One(tuple) => tuple.take(),
            Self::Many(iter) => iter.next(),
        }
    }
}
