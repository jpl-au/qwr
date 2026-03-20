# Claude Code Instructions

## Commit Rules

- Never commit this file (CLAUDE.md)
- Never commit `*-audit.md` files
- Never include `Co-Authored-By` trailers in commit messages
- Never commit files containing secrets (.env, credentials, etc.)
- Do not push to remote unless explicitly asked

## Working Style

When asked a question, STOP and answer it. Don't keep making changes while discussing.
Discuss tradeoffs and approaches before implementing.

## Language

Use British spelling in code comments, commit messages, documentation (guides, markdown
files), and all user-facing strings. This means `-ise` not `-ize`, `-our` not `-or`,
`-isation` not `-ization`, etc.

Code (variable names, function names, etc.) may use American English where conventional in Go.

## Build

Always format and vet before building:

```bash
go fmt ./...
go vet ./...
go build ./...
```

Never use `go build .` which writes a binary.

When editing Go files, do not worry about precise whitespace or indentation  - 
`gofmt` and `goimports` will normalise formatting automatically.

## Code Style

We follow Google's Go Style Guide principles. Go code should be simple for those
using, reading, and maintaining it. Written in the simplest way that accomplishes
its goals, both in terms of behaviour and performance.

- Is easy to read from top to bottom
- Does not assume that you already know what it is doing
- Does not assume that you can memorise all of the preceding code
- Does not have unnecessary levels of abstraction
- Does not have names that call attention to something mundane
- Makes the propagation of values and decisions clear to the reader
- Has comments that explain why, not what, the code is doing to avoid future deviation
- Has documentation that stands on its own
- Has useful errors and useful test failures
- May often be mutually exclusive with "clever" code

## Naming

Short, accurate, precise names. No verbose method names. Follow Go conventions.

### Receiver names

Receiver variable names must be:

- Short (usually one or two letters)
- Abbreviations for the type itself
- Applied consistently to every receiver for that type

| Long Name | Better Name |
|-----------|-------------|
| `func (tray Tray)` | `func (t Tray)` |
| `func (info *ResearchInfo)` | `func (ri *ResearchInfo)` |
| `func (this *ReportWriter)` | `func (w *ReportWriter)` |
| `func (self *Scanner)` | `func (s *Scanner)` |

### Constant names

Constant names must use MixedCaps like all other names in Go. Exported constants
start with uppercase, unexported with lowercase. This applies even when it breaks
conventions in other languages. Constant names should explain what the value
denotes, not be a derivative of their values.

### Initialisms

Words in names that are initialisms or acronyms (e.g., URL and NATO) should have
the same case. `URL` should appear as `URL` or `url` (as in `urlPony` or
`URLPony`), never as `Url`. Identifiers like `ID` and `DB` should also be
capitalised similarly.

In names with multiple initialisms (e.g. `XMLAPI`), each letter within a given
initialism should have the same case, but each initialism in the name does not
need to have the same case.

In names with an initialism containing a lowercase letter (e.g. `DDoS`, `iOS`,
`gRPC`), the initialism should appear as it would in standard prose, unless you
need to change the first letter for exportedness. In these cases, the entire
initialism should be the same case (e.g. `ddos`, `IOS`, `GRPC`).

### Getters

Function and method names should not use a `Get` or `get` prefix, unless the
underlying concept uses the word "get" (e.g. an HTTP GET). Prefer starting the
name with the noun directly, for example use `Counts` over `GetCounts`.

If the function involves performing a complex computation or executing a remote
call, a different word like `Compute` or `Fetch` can be used in place of `Get`,
to make it clear that the function call may take time and could block or fail.

### Variable names

The length of a name should be proportional to the size of its scope and inversely
proportional to the number of times it is used within that scope.

- A small scope is 1–7 lines
- A medium scope is 8–15 lines
- A large scope is 15–25 lines
- A very large scope is more than 25 lines

In general:

- Single-word names like `count` or `options` are a good starting point
- Additional words can disambiguate: `userCount` and `projectCount`
- Do not drop letters to save typing - `Sandbox` not `Sbx`, especially for exports
- Omit types from variable names: `userCount` not `numUsers` or `usersInt`; `users` not `userSlice`
- Omit words clear from context: inside a `UserCount` method, `count` or `c` suffices

### Single-letter variable names

Single-letter names minimise repetition but can make code opaque. Use them when the
full word is obvious and would be repetitive:

- Method receiver: one or two letters preferred
- Familiar names for common types: `r` for `io.Reader` / `*http.Request`, `w` for `io.Writer` / `http.ResponseWriter`
- Loop indices: `i`, `x`, `y`
- Short-scope loop identifiers: `for _, n := range nodes { ... }`

## Doc Comments

All top-level exported names must have doc comments, as should unexported type or
function declarations with unobvious behaviour or meaning. These comments should
be full sentences that begin with the name of the object being described.

### Package comments

Package comments must appear immediately above the package clause with no blank
line between the comment and the package name.

If there is no obvious primary file or if the package comment is extraordinarily
long, it is acceptable to put the doc comment in a file named `doc.go` with only
the comment and the package clause.
