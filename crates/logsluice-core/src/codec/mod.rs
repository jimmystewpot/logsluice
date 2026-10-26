//! Codecs and plan mapping for headers, tails, and emission tickets.

mod cursor;

pub mod header;
pub mod plan;
pub mod tail;
pub mod ticket;

pub use header::NumericHeader;
pub use plan::{AggregationPlan, HeaderLayout};
pub use tail::ExemplarTail;
pub use ticket::{EmitReason, Ticket, TicketEntry};
