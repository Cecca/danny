use measure::InnerProduct;

pub struct VectorWithNorm {
    data: Vec<f64>,
    norm: f64,
}

impl VectorWithNorm {
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    pub fn new(data: Vec<f64>) -> VectorWithNorm {
        let norm = InnerProduct::norm_2(&data);
        VectorWithNorm { data, norm }
    }

    pub fn data(&self) -> &Vec<f64> {
        &self.data
    }

    pub fn norm(&self) -> f64 {
        self.norm
    }
}
