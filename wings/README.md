# Wings CLI

The Wings CLI provides a command-line interface for managing and running the Wings message queue system.

## Installation

From the project root:

```bash
cargo build --release -p wings
```

The binary will be available at `target/release/wings`.

## Usage

### Development Mode

Start Wings in development mode:

```bash
wings dev
```

Options:
- `--bind <ADDRESS>`: Set the bind address (default: `127.0.0.1:8080`)

Example:
```bash
wings dev --bind 0.0.0.0:9000
```

### Admin Commands

The CLI provides several admin commands for managing tenants and namespaces:

#### Create a tenant

```bash
wings admin create-tenant <TENANT_NAME>
```

#### List all tenants

```bash
wings admin list-tenants
```

#### Create a namespace

```bash
wings admin create-namespace <TENANT_NAME> <NAMESPACE_NAME>
```

#### List namespaces for a tenant

```bash
wings admin list-namespaces <TENANT_NAME>
```

## Examples

```bash
# Start Wings in development mode
wings dev

# Create a new tenant
wings admin create-tenant my-company

# Create a namespace for the tenant
wings admin create-namespace my-company events

# List all tenants
wings admin list-tenants

# List namespaces for a specific tenant
wings admin list-namespaces my-company
```

## Development

This CLI follows the Wings project style guide and uses:

- `clap` with derive API for command-line parsing
- `tokio` for async runtime
- `error-stack` and `thiserror` for error handling
- `tokio-util::sync::CancellationToken` for graceful shutdown

The CLI is structured with:
- `main.rs`: Entry point and command routing
- `error.rs`: Error types and result type aliases
- `commands/`: Command implementations
  - `dev.rs`: Development mode command
  - `admin.rs`: Admin API commands