# Navigation Model

Inara's navigation is inspired by helix editor. This document defines the keybinding model so all agents implement consistent behavior.

## Modes

| Mode | Purpose | Enter | Exit |
|------|---------|-------|------|
| Normal | Browse schema | Default / `Esc` | — |
| Search | Fuzzy find symbols | `Space` submenu | `Esc` / select |
| HUD | Data glance overlay | `q` | `Esc` |
| Command | Ex-style commands | `:` | `Enter` / `Esc` |
| DefaultPrompt | Set column default value | `D` on column | `Enter` / `Esc` |
| Rename | Rename element | `r` on table/column | `Enter` / `Esc` |

## Normal Mode

### Movement

| Key | Action |
|-----|--------|
| `j` / `Down` | Move cursor down one line |
| `k` / `Up` | Move cursor up one line |
| `gg` | Jump to first element |
| `G` | Jump to last element |
| `Ctrl-d` | Half-page down |
| `Ctrl-u` | Half-page up |
| `Ctrl-f` | Full-page down |
| `Ctrl-b` | Full-page up |
| `Enter` | Toggle expand/collapse on focused table |
| `Tab` | Next table |
| `Shift-Tab` | Previous table |

### Space Menu (HUD)

`Space` opens the command palette:

| Key | Action |
|-----|--------|
| `Space f` | Fuzzy symbol search (all types) |
| `Space t` | Fuzzy table search |
| `Space c` | Fuzzy column search |
| `Space m` | Fuzzy migration search |
| `Space p` | Show pending migrations |
| `Space ?` | Show keybinding help |

### Goto Matrix (`g` prefix)

Context-sensitive: behavior depends on whether a table or column is focused.

#### Table Focused

| Key | Action |
|-----|--------|
| `g r` | Incoming foreign key references |
| `g o` | Outgoing foreign key references |
| `g i` | Indexes on this table |
| `g m` | Migrations affecting this table |
| `g c` | Jump to first column |
| `g t` | Types used by this table |

#### Column Focused

| Key | Action |
|-----|--------|
| `g r` | Incoming FK references to this column |
| `g d` | Jump to FK target definition |
| `g m` | Migrations affecting this column |
| `g t` | Jump to parent table |
| `g i` | Indexes containing this column |
| `g y` | Jump to enum/custom type definition |

### Actions

| Key | Action |
|-----|--------|
| `q` | Open Query HUD for focused element |
| `e` | Edit focused table in `$EDITOR` |
| `r` | Rename focused element (explicit) |
| `:` | Enter command mode |

### Quick Actions (column-focused only)

| Key | Action |
|-----|--------|
| `n` | Toggle nullable |
| `u` | Toggle unique constraint (single-column) |
| `i` | Toggle index (single-column) |
| `D` | Set or clear default value (enters DefaultPrompt mode) |

## Command Mode

| Command | Action |
|---------|--------|
| `:q` | Quit |
| `:w` | Write migration with safety checks (dialog on destructive changes) |
| `:w!` | Write migration without confirmation |
| `:w <desc>` | Write migration with description (safety checks) |
| `:w! <desc>` | Write migration with description (no confirmation) |
| `:ai` | LLM edit prompt (optional) |
| `:generate-down` | Generate down migration via LLM (optional) |

### `:w` Safety Dialog

When `:w` detects potentially destructive changes (e.g., adding NOT NULL to a column with existing NULL rows), it presents a dialog with options:

- **Cancel** — Return to editing (e.g., to add a default value first)
- **Accept** — Write the migration as-is (equivalent to `:w!`)
- **Use AI** — Generate a data migration via LLM (only available if AI is configured)

Migration generation is disabled when there are pending (unapplied) migrations. Apply pending migrations first (`sqlx migrate run`) before generating new ones.

## Implementation Notes

- Key sequences (like `gg`, `g r`) use a pending-key state. After pressing `g`, the app waits for the second key with a short timeout.
- The space menu renders as an overlay listing available actions. Pressing the submenu key immediately executes.
- Focus tracking: the app always knows what type of element is focused (Table, Column, Index, Constraint) and adjusts available actions accordingly.
- All keybindings should be discoverable via the space menu or `:help`.
