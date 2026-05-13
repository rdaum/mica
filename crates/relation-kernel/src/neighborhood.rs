use crate::{KernelError, RelationId, Snapshot, Transaction, Tuple};
use mica_var::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubjectFact {
    pub subject: Value,
    pub relation: RelationId,
    pub tuple: Tuple,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MentionedFact {
    pub identity: Value,
    pub relation: RelationId,
    pub position: u16,
    pub tuple: Tuple,
}

impl Snapshot {
    pub fn subject_facts(&self, subject: &Value) -> Result<Vec<SubjectFact>, KernelError> {
        let mut facts = Vec::new();
        for (relation, state) in &self.relations {
            if state.metadata().arity() == 0 {
                continue;
            }
            let mut bindings = vec![None; state.metadata().arity() as usize];
            bindings[0] = Some(subject.clone());
            facts.extend(state.scan(&bindings)?.into_iter().map(|tuple| SubjectFact {
                subject: subject.clone(),
                relation: *relation,
                tuple,
            }));
        }
        Ok(facts)
    }

    pub fn mentioned_facts(&self, identity: &Value) -> Result<Vec<MentionedFact>, KernelError> {
        let mut facts = Vec::new();
        for (relation, state) in &self.relations {
            for position in 0..state.metadata().arity() {
                let mut bindings = vec![None; state.metadata().arity() as usize];
                bindings[position as usize] = Some(identity.clone());
                facts.extend(
                    state
                        .scan(&bindings)?
                        .into_iter()
                        .map(|tuple| MentionedFact {
                            identity: identity.clone(),
                            relation: *relation,
                            position,
                            tuple,
                        }),
                );
            }
        }
        Ok(facts)
    }
}

impl Transaction<'_> {
    pub fn subject_facts(&self, subject: &Value) -> Result<Vec<SubjectFact>, KernelError> {
        let mut facts = Vec::new();
        for (relation, state) in &self.base.relations {
            if state.metadata().arity() == 0 {
                continue;
            }
            let mut bindings = vec![None; state.metadata().arity() as usize];
            bindings[0] = Some(subject.clone());
            facts.extend(
                self.scan(*relation, &bindings)?
                    .into_iter()
                    .map(|tuple| SubjectFact {
                        subject: subject.clone(),
                        relation: *relation,
                        tuple,
                    }),
            );
        }
        Ok(facts)
    }

    pub fn mentioned_facts(&self, identity: &Value) -> Result<Vec<MentionedFact>, KernelError> {
        let mut facts = Vec::new();
        for (relation, state) in &self.base.relations {
            for position in 0..state.metadata().arity() {
                let mut bindings = vec![None; state.metadata().arity() as usize];
                bindings[position as usize] = Some(identity.clone());
                facts.extend(self.scan(*relation, &bindings)?.into_iter().map(|tuple| {
                    MentionedFact {
                        identity: identity.clone(),
                        relation: *relation,
                        position,
                        tuple,
                    }
                }));
            }
        }
        Ok(facts)
    }
}
