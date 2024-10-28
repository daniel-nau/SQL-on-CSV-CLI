use std::collections::HashMap;
use std::fmt::Debug;

pub trait Aggregate: Debug {
    fn apply(&mut self, value: f64);
    fn result(&self) -> f64;
}

#[derive(Debug)]
pub struct Sum {
    total: f64,
}

impl Sum {
    pub fn new() -> Self {
        Sum { total: 0.0 }
    }
}

impl Aggregate for Sum {
    fn apply(&mut self, value: f64) {
        self.total += value;
    }

    fn result(&self) -> f64 {
        self.total
    }
}

#[derive(Debug)]
pub struct Avg {
    total: f64,
    count: usize,
}

impl Avg {
    pub fn new() -> Self {
        Avg { total: 0.0, count: 0 }
    }
}

impl Aggregate for Avg {
    fn apply(&mut self, value: f64) {
        self.total += value;
        self.count += 1;
    }

    fn result(&self) -> f64 {
        if self.count > 0 {
            self.total / self.count as f64
        } else {
            f64::NAN
        }
    }
}

#[derive(Debug)]
pub struct Min {
    min_value: f64,
    initialized: bool,
}

impl Min {
    pub fn new() -> Self {
        Min { min_value: f64::INFINITY, initialized: false }
    }
}

impl Aggregate for Min {
    fn apply(&mut self, value: f64) {
        if !self.initialized || value < self.min_value {
            self.min_value = value;
            self.initialized = true;
        }
    }

    fn result(&self) -> f64 {
        if self.initialized {
            self.min_value
        } else {
            f64::NAN
        }
    }
}

#[derive(Debug)]
pub struct Max {
    max_value: f64,
    initialized: bool,
}

impl Max {
    pub fn new() -> Self {
        Max { max_value: f64::NEG_INFINITY, initialized: false }
    }
}

impl Aggregate for Max {
    fn apply(&mut self, value: f64) {
        if !self.initialized || value > self.max_value {
            self.max_value = value;
            self.initialized = true;
        }
    }

    fn result(&self) -> f64 {
        if self.initialized {
            self.max_value
        } else {
            f64::NAN
        }
    }
}
#[derive(Debug)]
pub struct Count {
    count: usize,
}

impl Count {
    pub fn new() -> Self {
        Count { count: 0 }
    }
}

impl Aggregate for Count {
    fn apply(&mut self, _value: f64) {
        self.count += 1;
    }

    fn result(&self) -> f64 {
        self.count as f64
    }
}

#[derive(Debug)]
pub struct Aggregates {
    pub functions: HashMap<String, Box<dyn Aggregate>>,
}

impl Aggregates {
    pub fn new() -> Self {
        Aggregates {
            functions: HashMap::new(),
        }
    }

    pub fn add_function(&mut self, column_name: String, aggregate: Box<dyn Aggregate>) {
        self.functions.insert(column_name, aggregate);
    }

    pub fn results(&self, columns: &[String]) -> HashMap<String, f64> {
        columns
            .iter()
            .map(|col| {
                let result = self.functions.get(col).map_or(f64::NAN, |agg| agg.result());
                (col.clone(), result)
            })
            .collect()
    }
}