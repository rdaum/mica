mod scalar;

use crate::BuiltinRegistry;

pub(crate) fn install_scalar_builtins(registry: BuiltinRegistry) -> BuiltinRegistry {
    scalar::install(registry)
}
