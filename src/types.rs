extern crate clap;

use clap::arg_enum;

arg_enum! {
    #[derive(Debug)]
    #[allow(non_camel_case_types)]
    pub enum SyncDirection {
        up,
        down,
    }
}
