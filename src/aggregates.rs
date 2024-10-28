use std::collections::HashMap;

pub trait Aggregate {
    fn apply(&mut self, value: f64);
    fn result(&self) -> f64;
}

pub struct Min {
    min: Option<f64>,
}

impl Min {
    pub fn new() -> Self {
        Min { min: None }
    }
}

impl Aggregate for Min {
    fn apply(&mut self, value: f64) {
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
    }

    fn result(&self) -> f64 {
        self.min.unwrap_or(f64::NAN)
    }
}

pub struct Max {
    max: Option<f64>,
}

impl Max {
    pub fn new() -> Self {
        Max { max: None }
    }
}

impl Aggregate for Max {
    fn apply(&mut self, value: f64) {
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
    }

    fn result(&self) -> f64 {
        self.max.unwrap_or(f64::NAN)
    }
}

pub struct Sum {
    sum: f64,
}

impl Sum {
    pub fn new() -> Self {
        Sum { sum: 0.0 }
    }
}

impl Aggregate for Sum {
    fn apply(&mut self, value: f64) {
        self.sum += value;
    }

    fn result(&self) -> f64 {
        self.sum
    }
}

pub struct Avg {
    sum: f64,
    count: usize,
}

impl Avg {
    pub fn new() -> Self {
        Avg { sum: 0.0, count: 0 }
    }
}

impl Aggregate for Avg {
    fn apply(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    fn result(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            f64::NAN
        }
    }
}

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

pub struct Aggregates {
    pub functions: HashMap<String, Box<dyn Aggregate>>, // Column name mapped to its aggregate function
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

    pub fn results(&self) -> HashMap<String, f64> {
        self.functions
            .iter()
            .map(|(col, agg)| (col.clone(), agg.result()))
            .collect()
    }
}
