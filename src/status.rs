use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use dicom::core::Tag;
use tracing::{debug, error, warn};
use crate::error::DimseError;

pub(crate) use crate::definitions::{FailureCode, WarningCode};
pub type Status = Result<StatusOk,StatusFailure>;

#[derive(Clone,Debug)]
pub enum StatusOk{
	Success(StatusSuccess),
	Pending(StatusPending),
	Warning(StatusWarning),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct  StatusSuccess;
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StatusPending {
	pub succeeded:Option<u16>,
	pub warn:Option<u16>,
	pub fail:Option<u16>,
	pub to_do:Option<u16>,
	offset:u16,
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StatusWarning{
	code: WarningCode,
	pub offending_element:HashSet<Tag>,
	pub comment:Option<String>,
	offset:u16,
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StatusFailure{
	code:FailureCode,
	pub offending_element:HashSet<Tag>,
	pub comment:Option<String>,
	offset:u16,
}

pub fn success() -> StatusSuccess {Default::default()}
pub fn pending(succeeded:u16, warn:u16, fail:u16, to_do:u16) -> StatusPending {
	StatusPending{
		succeeded: Some(succeeded),
		warn: Some(warn),
		fail: Some(fail),
		to_do: Some(to_do),
		offset: 0,
	}
}
pub fn warning(code: WarningCode) -> StatusWarning {StatusWarning{code, ..Default::default()}}
pub fn failure(code:FailureCode) -> StatusFailure {StatusFailure{code, ..Default::default()}}


////////////////////// Tooling /////////////////////////

/////////////////////// Code ///////////////////////////
pub(crate) trait Code
{
	fn code(&self) -> u16;
	fn try_from_code(code:u16) -> Option<Self> where Self:Sized;
	fn offset(self, offset:u16) -> Self;
	fn get_offset(&self) -> u16;

}
impl Code for StatusSuccess{
	fn code(&self) -> u16 {0x0000}
	fn try_from_code(code:u16) -> Option<Self>{
		if code==0x0000{Some(Self::default())}else{None}
	}
	fn offset(self, _:u16) -> Self {panic!("Invalid call to set offset")}
	fn get_offset(&self) -> u16 {debug!("Needlessly calling get_offset on a SUCCESS state");0}
}
impl Code for StatusPending{
	fn code(&self) -> u16 {0xff00+self.offset}
	fn try_from_code(code:u16) -> Option<Self>{
		if let 0xFF00 ..= 0xFFFF = code {
			Some(StatusPending{offset:code-0xFF00, ..Default::default()})
		} else {None}
	}
	fn offset(mut self, offset:u16) -> Self {self.offset=offset;self}
	fn get_offset(&self) -> u16 {self.offset}
}
impl Code for StatusWarning {
	fn code(&self) -> u16 { u16::from(self.code.to_owned()) }
	fn try_from_code(code: u16) -> Option<Self> {
		if let 0x0001..=0x00FF = code {
			if let Ok(code) = WarningCode::try_from(code){
				Some(StatusWarning{code, ..Default::default()})
			} else if let 0xB000 ..= 0xBFFF = code {
				Some(StatusWarning{code:WarningCode::DataCoercion, offset:code-0xB000,..Default::default()})
			} else {
				warn!("unknown warning code: 0x{code:04X}");
				Some(StatusWarning{code:WarningCode::DataCoercion,..Default::default()})
			}
		} else { None }
	}
	fn offset(mut self, offset:u16) -> Self {self.offset=offset;self}
	fn get_offset(&self) -> u16 {self.offset}
}

impl Code for StatusOk {
	fn code(&self) -> u16 {
		match self {
			StatusOk::Success(_) => 0x0000,
			StatusOk::Pending(p) => 0xff00+p.offset,
			StatusOk::Warning(w) => u16::from(w.code.to_owned())+w.offset,
		}
	}

	fn try_from_code(code: u16) -> Option<Self> where Self: Sized
	{
		match code {
			0x0000 => Some(StatusOk::Success(StatusSuccess::default())),
			0x0001 ..= 0x00ff => StatusWarning::try_from_code(code).map(|w| StatusOk::Warning(w)),
			0xFF00 ..= 0xFFff => StatusPending::try_from_code(code).map(|p| StatusOk::Pending(p)),
			_ => None
		}
	}

	fn offset(self, offset: u16) -> Self {
		match self {
			StatusOk::Success(_) => self,
			StatusOk::Pending(p) => StatusOk::Pending(p.offset(offset)),
			StatusOk::Warning(w) => StatusOk::Warning(w.offset(offset)),
		}
	}

	fn get_offset(&self) -> u16 {
		match self {
			StatusOk::Success(_) => 0,
			StatusOk::Pending(p) => 0xff00+p.offset,
			StatusOk::Warning(w) => u16::from(w.code.to_owned())+w.offset,
		}
	}
}
impl Code for StatusFailure{
	fn code(&self) -> u16 {u16::from(self.code.to_owned())+self.offset}
	fn try_from_code(code:u16) -> Option<Self>{
		FailureCode::try_from(code).ok().map(|matched_code|{
			StatusFailure{code:matched_code,..Default::default()}
		})
		.or_else(|| {
			match code {
				0x0100..=0x01ff => Some(StatusFailure {offset: code - 0x0100, ..Default::default() }),
				0xA700..=0xA7ff => Some(StatusFailure {code:FailureCode::OutOfResources, offset: code - 0xA700, ..Default::default() }),
				0xA900..=0xA9ff => Some(StatusFailure {code:FailureCode::SOPClassDoesNotMatch, offset: code - 0xA900, ..Default::default() }),
				0xC000..=0xCfff => Some(StatusFailure {code:FailureCode::CannotUnderstand, offset: code - 0xA900, ..Default::default() }),
				_ => {
					warn!("unknown error code: 0x{code:04X}");
					None
				}
			}
		})
	}

	fn offset(mut self, offset: u16) -> Self {self.offset=offset;self}
	fn get_offset(&self) -> u16 {self.offset}
}
impl Code for Result<StatusOk,StatusFailure> {
	fn code(&self) -> u16 {
		match self {
			Ok(ok) => ok.code(),
			Err(f) => u16::from(f.code.to_owned())+f.offset,
		}
	}
	fn try_from_code(code:u16) -> Option<Self> {
		match code {
			0x0000 | 0x0001 ..= 0x00ff => Ok(StatusOk::try_from_code(code)).transpose(),
			0x0100 ..=0xfe00 => StatusFailure::try_from_code(code).map(|f| Err(f)),
			_ => None
		}
	}
	fn offset(self, offset:u16) -> Self {
		match self {
			Ok(ok) => Ok(ok.offset(offset)),
			Err(e) => Err(e.offset(offset)),
		}
	}

	fn get_offset(&self) -> u16 {
		match self {
			Ok(ok) => ok.get_offset(),
			Err(e) => e.get_offset(),
		}
	}
}

///////////////////////////////////// comment /////////////////////////////////////

pub trait Comment{
	fn comment(self, cmt:impl ToString) -> Self;
	fn get_comment(&self) -> Option<&String>;
}

impl Comment for StatusFailure {
	fn comment(mut self, cmt:impl ToString) -> Self {
		self.comment.replace(cmt.to_string());
		self
	}
	fn get_comment(&self) -> Option<&String> {self.comment.as_ref()}
}
impl Comment for StatusWarning {
	fn comment(mut self, cmt:impl ToString) -> Self {
		self.comment.replace(cmt.to_string());
		self
	}
	fn get_comment(&self) -> Option<&String> {self.comment.as_ref()}
}

///////////////////////////////////// offending /////////////////////////////////////

pub trait Offending{
	fn offend(&mut self, offender:impl IntoIterator<Item=Tag>);
	fn offending(mut self, offender:impl IntoIterator<Item=Tag>) -> Self where Self: Sized{
		self.offend(offender);
		self
	}
}

impl Offending for StatusWarning{
	fn offend(&mut self, offender: impl IntoIterator<Item=Tag>) {
		for t in offender{
			self.offending_element.insert(t);
		}
	}
}

impl Offending for StatusFailure{
	fn offend(&mut self, offender: impl IntoIterator<Item=Tag>) {
		for t in offender{
			self.offending_element.insert(t);
		}
	}
}

///////////////////////////////////// Conversion /////////////////////////////////////

impl From<StatusSuccess> for StatusOk {
	fn from(s: StatusSuccess) -> Self {StatusOk::Success(s)}
}
impl From<StatusPending> for StatusOk {
	fn from(p: StatusPending) -> Self {StatusOk::Pending(p)}
}
impl From<StatusWarning> for StatusOk {
	fn from(w: StatusWarning) -> Self {StatusOk::Warning(w)}
}

impl<T> From<StatusSuccess> for Result<StatusOk,T> {
	fn from(s: StatusSuccess) -> Self {Ok(s.into())}
}
impl<T> From<StatusPending> for Result<StatusOk,T> {
	fn from(p: StatusPending) -> Self {Ok(p.into())}
}
impl<T> From<StatusWarning> for Result<StatusOk,T> {
	fn from(w: StatusWarning) -> Self {Ok(w.into())}
}

impl From<DimseError> for StatusFailure
{
	fn from(err: DimseError) -> Self { Self::default().comment(err.to_string()) }
}

///////////////////////////////////// Display /////////////////////////////////////
impl AsRef<str> for StatusSuccess {
	fn as_ref(&self) -> &str {"Success"}
}
impl AsRef<str> for StatusPending {
	fn as_ref(&self) -> &str {"Pending"}
}
impl AsRef<str> for StatusWarning {
	fn as_ref(&self) -> &str {"Warning"}
}
impl AsRef<str> for StatusFailure {
	fn as_ref(&self) -> &str {"Failure"}
}

impl Display for StatusSuccess {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.write_str(self.as_ref())
	}
}

impl Display for StatusPending {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let offset_str = if self.get_offset() > 0 {format!("+{}", self.get_offset())} else {"".to_string()};
		f.write_fmt(format_args!("Pending{}", offset_str))
	}
}
impl Display for StatusWarning
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let code = self.code();
		let offset_str = if self.get_offset() > 0 {format!("+{}", self.get_offset())} else {"".to_string()};
		self.get_comment().map(
			|c|f.write_fmt(format_args!(r#"Warning "{}{}", comment: "{c}""#,code,offset_str))
		).unwrap_or(
			f.write_fmt(format_args!(r#"Warning: "{}{}""#,code,offset_str))
		)
	}
}

impl Display for StatusOk
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result
	{
		match self {
			StatusOk::Success(_) => Display::fmt(&success(), f),
			StatusOk::Pending(p) => Display::fmt(p, f),
			StatusOk::Warning(w) => Display::fmt(w, f),
		}
	}
}

impl Display for StatusFailure{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result
	{
		let offset = if self.offset > 0 {format!("+{}",self.offset)} else {"".to_string()};
		if let Some(c) = &self.comment{
			f.write_fmt(format_args!(r#"Error "{}{}", comment: "{c}""#,self.code,offset))
		} else { f.write_fmt(format_args!(r#"Error: "{}{}""#,self.code,offset)) }

	}
}

impl snafu::Error for StatusFailure{}


