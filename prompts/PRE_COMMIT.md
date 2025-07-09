# Pre-Commit Work

Before committing the code, you need to make sure it follows all the best practices and coding standards.

### Remove Redundant 'What' Comments

If the code contains any comment that describes WHAT the next line does, it should be removed.

<example original>
// Check if the parent tenant exists
if !store.tenants.contains_key(tenant_id) {
    return Err(error_stack::Report::new(AdminError::NotFound {
        resource: "tenant",
        message: tenant_id.to_string(),
    })
    .attach_printable("parent tenant must exist before creating namespace"));
}

// Check if namespace already exists
if store.namespaces.contains_key(&namespace_key) {
    return Err(error_stack::Report::new(AdminError::AlreadyExists {
        resource: "namespace",
        message: name.id().to_string(),
    }));
}

// Create and store the namespace
let namespace = Namespace::new(name, options);
    store.namespaces.insert(namespace_key, namespace.clone());
</example>

<example result>
if !store.tenants.contains_key(tenant_id) {
    return Err(error_stack::Report::new(AdminError::NotFound {
        resource: "tenant",
        message: tenant_id.to_string(),
    })
    .attach_printable("parent tenant must exist before creating namespace"));
}

if store.namespaces.contains_key(&namespace_key) {
    return Err(error_stack::Report::new(AdminError::AlreadyExists {
        resource: "namespace",
        message: name.id().to_string(),
    }));
}

let namespace = Namespace::new(name, options);
    store.namespaces.insert(namespace_key, namespace.clone());
</example>

### Remove doctests

Remove doctests from the module. Wings is a standalone application and it's not concerned with documenting its internals.

### Organize imports, definitions, and implementations

**Imports**

Imports should be organized in three distinct groups, separated by a blank line:

- Standard library imports
- Third-party library imports
- Local module imports

**Definitions and implementations**

Move around definitions and implementations to follow the following order:

- Constants and (lazy) static variables
- Public definitions (struct, enum, trait, types, etc.)
- Private definitions (struct, enum, trait, types, etc.)
- Implementations for public types
- Trait implementations for public types
- Implementations for private types
- Trait implementations for private types

The order of types/structs/etc should be consistent throughout the module. Use your judgement to organize them, usually we put the most important ones first.

### Run clippy

Run `cargo clippy` to check for common mistakes and style issues. Fix any warnings that are reported and repeat.

**IF YOU DON'T KNOW HOW TO FIX THE ISSUE, STOP AND ASK FOR HELP.**

### Format code

Run `cargo fmt` to format the code. This command should never fail.

## Checklist

Make sure you run all the checks (in order) before finishing the task.

Do this for every file that has been modified and not committed.

<check_list>
- Remove Redundant 'What' Comments
- Remove doctests
- Organize imports, definitions, and implementations
- Run clippy
- Format code
</check_list>
