use std::{fmt, io};

use nu_ansi_term::{Color, Style};
use tracing::{field, span, Event, Level, Subscriber};

use tracing_subscriber::field::{VisitFmt, VisitOutput};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{prelude::*, registry::LookupSpan};

pub struct DnaFormat {
    time_format: time::format_description::OwnedFormatItem,
}

impl<S, N> FormatEvent<S, N> for DnaFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let meta = event.metadata();

        write!(
            writer,
            "{}",
            FmtLevel::new(meta.level(), writer.has_ansi_escapes())
        )?;
        writer.write_char(' ')?;
        if self.format_time(&mut writer).is_err() {
            write!(writer, "[<unknown-timestamp>]")?;
        };
        writer.write_char(' ')?;

        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

impl<'w> FormatFields<'w> for DnaFormat {
    fn format_fields<R: __tracing_subscriber_field_RecordFields>(
        &self,
        writer: Writer<'w>,
        fields: R,
    ) -> fmt::Result {
        let mut v = DnaFormatVisitor::new(writer, true);
        fields.record(&mut v);
        v.finish()
    }

    fn add_fields(
        &self,
        current: &'w mut tracing_subscriber::fmt::FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let empty = current.is_empty();
        let writer = current.as_writer();
        let mut v = DnaFormatVisitor::new(writer, empty);
        fields.record(&mut v);
        v.finish()
    }
}

struct DnaFormatVisitor<'a> {
    writer: Writer<'a>,
    is_empty: bool,
    style: Style,
    result: std::fmt::Result,
}

impl<'a> DnaFormatVisitor<'a> {
    fn new(writer: Writer<'a>, is_empty: bool) -> Self {
        Self {
            writer,
            is_empty,
            style: Style::new(),
            result: Ok(()),
        }
    }

    fn write_padded(&mut self, v: &impl fmt::Debug) {
        let padding = if self.is_empty {
            self.is_empty = false;
            ""
        } else {
            " "
        };

        self.result = write!(self.writer, "{}{:?}", padding, v);
    }
}

impl field::Visit for DnaFormatVisitor<'_> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{:0<60}", value))
        } else {
            self.record_debug(field, &value)
        }
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }

        let value = format!("{:?}", value);
        match field.name() {
            "message" => {
                self.write_padded(&format_args!("{}{:<40}", self.style.prefix(), value));
            }
            name => {
                let color = if field.name() == "error" {
                    Color::Red
                } else {
                    Color::Blue
                };

                if self.writer.has_ansi_escapes() {
                    self.write_padded(&format_args!(
                        "{}{}={}",
                        self.style.prefix(),
                        name,
                        color.paint(value)
                    ));
                } else {
                    self.write_padded(&format_args!("{}{}={}", self.style.prefix(), name, value));
                }
            }
        }
    }
}

impl VisitOutput<std::fmt::Result> for DnaFormatVisitor<'_> {
    fn finish(mut self) -> std::fmt::Result {
        write!(&mut self.writer, "{}", self.style.suffix())?;
        self.result
    }
}

impl VisitFmt for DnaFormatVisitor<'_> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer
    }
}

impl DnaFormat {
    pub fn format_time(&self, writer: &mut Writer<'_>) -> std::fmt::Result {
        let now = time::OffsetDateTime::from(std::time::SystemTime::now());
        let mut w = WriteAdaptor { fmt_writer: writer };
        now.format_into(&mut w, &self.time_format)
            .map_err(|_| std::fmt::Error)?;
        Ok(())
    }
}

struct FmtLevel<'a> {
    level: &'a Level,
    ansi: bool,
}

struct WriteAdaptor<'a> {
    fmt_writer: &'a mut dyn fmt::Write,
}

impl<'a> FmtLevel<'a> {
    fn new(level: &'a Level, ansi: bool) -> Self {
        Self { level, ansi }
    }
}

impl std::fmt::Display for FmtLevel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.ansi {
            match *self.level {
                Level::TRACE => write!(f, "[{}]", Color::Purple.paint("TRACE")),
                Level::DEBUG => write!(f, "[{}]", Color::Blue.paint("DEBUG")),
                Level::INFO => write!(f, "[{}]", Color::Green.paint("INFO")),
                Level::WARN => write!(f, "[{}]", Color::Yellow.paint("WARN")),
                Level::ERROR => write!(f, "[{}]", Color::Red.paint("ERROR")),
            }
        } else {
            match *self.level {
                Level::TRACE => write!(f, "[TRACE]"),
                Level::DEBUG => write!(f, "[DEBUG]"),
                Level::INFO => write!(f, "[INFO]"),
                Level::WARN => write!(f, "[WARN]"),
                Level::ERROR => write!(f, "[ERROR]"),
            }
        }
    }
}

impl io::Write for WriteAdaptor<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let s =
            std::str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.fmt_writer
            .write_str(s)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(s.as_bytes().len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Default for DnaFormat {
    fn default() -> Self {
        let time_format = time::format_description::parse_owned::<2>(
            r#"\[[month]-[day]|[hour]:[minute]:[second].[subsecond digits:3]\]"#,
        )
        .expect("failed to parse time format");

        Self { time_format }
    }
}
