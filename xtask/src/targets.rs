#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Target {
    pub(crate) label: &'static str,
    pub(crate) triple: &'static str,
}

pub(crate) const SUPPORTED_TARGETS: [Target; 4] = [
    Target {
        label: "Linux x86_64",
        triple: "x86_64-unknown-linux-gnu",
    },
    Target {
        label: "Linux aarch64",
        triple: "aarch64-unknown-linux-gnu",
    },
    Target {
        label: "macOS aarch64",
        triple: "aarch64-apple-darwin",
    },
    Target {
        label: "Windows x86_64",
        triple: "x86_64-pc-windows-msvc",
    },
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn supported_release_targets_are_unique_and_include_linux_arm64() {
        let triples = SUPPORTED_TARGETS
            .iter()
            .map(|target| target.triple)
            .collect::<BTreeSet<_>>();
        assert_eq!(triples.len(), SUPPORTED_TARGETS.len());
        assert!(triples.contains("aarch64-unknown-linux-gnu"));
    }
}
