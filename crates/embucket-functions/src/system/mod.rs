pub mod cancel_query;
pub mod errors;
pub mod sleep;
pub mod typeof_func;

use datafusion_common::Result;
use datafusion_expr::registry::FunctionRegistry;
pub use errors::Error;

/// Registers specific system-level User-Defined Functions (UDFs) into the provided `FunctionRegistry`. 
/// This includes functions like `typeof_func`, `cancel_query`, and a `sleep` function, which is explicitly marked for non-production usage to simulate delays for testing purposes. 
/// It returns `Ok(())` if all UDFs are successfully registered or an error if registration fails.
pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(typeof_func::get_udf())?;
    registry.register_udf(cancel_query::get_udf())?;
    // sleep is not for production use, returned udf created with `create_udf`
    registry.register_udf(sleep::get_udf())?;
    Ok(())
}


/// An empty placeholder function, serving as a shim. 
/// This function currently performs no operations and may be intended for future implementation. 
/// It provides a no-op method in contexts where a trait or interface requires it.
pub fn shim() {
 



    


    
}
