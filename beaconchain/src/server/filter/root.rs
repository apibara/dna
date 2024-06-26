use apibara_dna_protocol::beaconchain;

pub struct Filter {
    header: HeaderFilter,
}

pub struct HeaderFilter {
    always: bool,
}

impl Filter {
    pub fn needs_linear_scan(&self) -> bool {
        self.header.needs_linear_scan()
    }

    pub fn has_required_header(&self) -> bool {
        self.header.always
    }
}

impl HeaderFilter {
    pub fn needs_linear_scan(&self) -> bool {
        self.always
    }
}

impl Default for HeaderFilter {
    fn default() -> Self {
        Self { always: false }
    }
}

impl From<beaconchain::Filter> for Filter {
    fn from(value: beaconchain::Filter) -> Self {
        let header = value.header.map(HeaderFilter::from).unwrap_or_default();
        Self { header }
    }
}

impl From<beaconchain::HeaderFilter> for HeaderFilter {
    fn from(value: beaconchain::HeaderFilter) -> Self {
        Self {
            always: value.always.unwrap_or(false),
        }
    }
}
