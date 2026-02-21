use std::fmt::{Display, Formatter};
use dicom::dictionary_std::uids::*;
use dicom::object::Tag;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum_macros::{AsRefStr, Display};
use thiserror::Error;
use tracing::error;
use crate::error::DimseError;

/// A list of supported abstract syntaxes for storage services
#[allow(deprecated)]
pub static STORAGE_SYNTAXES: &[&str] = &[ //https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_B.5.html
	CT_IMAGE_STORAGE,
	ENHANCED_CT_IMAGE_STORAGE,
	STANDALONE_CURVE_STORAGE,
	STANDALONE_OVERLAY_STORAGE,
	SECONDARY_CAPTURE_IMAGE_STORAGE,
	ULTRASOUND_IMAGE_STORAGE_RETIRED,
	NUCLEAR_MEDICINE_IMAGE_STORAGE_RETIRED,
	MR_IMAGE_STORAGE,
	ENHANCED_MR_IMAGE_STORAGE,
	MR_SPECTROSCOPY_STORAGE,
	ENHANCED_MR_COLOR_IMAGE_STORAGE,
	ULTRASOUND_MULTI_FRAME_IMAGE_STORAGE_RETIRED,
	COMPUTED_RADIOGRAPHY_IMAGE_STORAGE,
	DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
	DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
	ENCAPSULATED_PDF_STORAGE,
	ENCAPSULATED_CDA_STORAGE,
	ENCAPSULATED_STL_STORAGE,
	GRAYSCALE_SOFTCOPY_PRESENTATION_STATE_STORAGE,
	POSITRON_EMISSION_TOMOGRAPHY_IMAGE_STORAGE,
	BREAST_TOMOSYNTHESIS_IMAGE_STORAGE,
	BREAST_PROJECTION_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
	BREAST_PROJECTION_X_RAY_IMAGE_STORAGE_FOR_PROCESSING,
	ENHANCED_PET_IMAGE_STORAGE,
	RT_IMAGE_STORAGE,
	NUCLEAR_MEDICINE_IMAGE_STORAGE,
	ULTRASOUND_MULTI_FRAME_IMAGE_STORAGE,
	MULTI_FRAME_SINGLE_BIT_SECONDARY_CAPTURE_IMAGE_STORAGE,
	MULTI_FRAME_GRAYSCALE_BYTE_SECONDARY_CAPTURE_IMAGE_STORAGE,
	MULTI_FRAME_GRAYSCALE_WORD_SECONDARY_CAPTURE_IMAGE_STORAGE,
	MULTI_FRAME_TRUE_COLOR_SECONDARY_CAPTURE_IMAGE_STORAGE,
	BASIC_TEXT_SR_STORAGE,
	ENHANCED_SR_STORAGE,
	COMPREHENSIVE_SR_STORAGE,
	VERIFICATION,
];

#[allow(deprecated)]
pub static QUERY_RETRIEVE_SYNTAXES: &[&str] = &[ //https://dicom.nema.org/dicom/2013/output/chtml/part04/sect_B.5.html
	STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
	STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE,
	STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET,
	PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND,
	PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE,
	PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET,
];

// https://dicom.nema.org/dicom/2013/output/chtml/part07/chapter_E.html
#[derive(Debug, Display, AsRefStr, Clone, Eq, PartialEq, TryFromPrimitive,IntoPrimitive)]
#[repr(u16)]
#[allow(non_camel_case_types)]
pub enum RequestCode {
	C_STORE = 0x0001,
	C_GET = 0x0010,
	C_FIND = 0x0020,
	C_MOVE = 0x0021,
	C_ECHO = 0x0030,
	C_CANCEL = 0x0FFF,
	N_EVENT_REPORT = 0x0100,
	N_GET = 0x0110,
	N_SET = 0x0120,
	N_ACTION = 0x0130,
	N_CREATE = 0x0140,
	N_DELETE = 0x0150,
}

//https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.html#table_C.4-1
//https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.html
// https://dicom.nema.org/dicom/2013/output/chtml/part07/sect_C.5.html

// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_C.html
// https://dicom.nema.org/medical/dicom/current/output/pdf/part07.pdf

pub type Status = Result<StatusOk, StatusFailure>;
#[derive(Debug, AsRefStr, Clone, PartialEq)]
pub enum StatusOk
{
	Success, //0x0000
	Pending { //0xff00
		succeeded:Option<u16>,
		warn:Option<u16>,
		fail:Option<u16>,
		to_do:Option<u16>,
	},
	Warning{
		code:StatusWarning,
		offending_element:Option<Vec<Tag>>,
		comment:Option<String>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatusFailure{
	pub code:FailureCode,
	pub offending_element:Option<Vec<Tag>>,
	pub comment:Option<String>,
}
impl StatusFailure{
	pub fn new(code:FailureCode, comment:Option<String>) -> StatusFailure {
		StatusFailure{code, offending_element: None, comment}
	}
}

#[derive(Debug, Clone, Eq, PartialEq, TryFromPrimitive,IntoPrimitive,Error)]
#[repr(u16)]
pub enum StatusWarning {
	#[error("Attribute List warning")]
	AttributeListWarning = 0x0107,
	#[error("Attribute Value out of range")]
	AttributeValueOutOfRange = 0x0116,
	#[error("Warning A")]
	Warning = 0x0001,
	#[error("Coercion of Data Elements")]
	DataCoercion = 0xB000, //bxxx
	#[error("Elements Discarded")]
	ElementsDiscarded = 0xB006,
	#[error("Data Set does not match SOP Class")]
	DataSetDoesNotMatch = 0xB007,
}

#[derive(Debug, Clone, Eq, PartialEq, TryFromPrimitive,IntoPrimitive,Error)]
#[repr(u16)]
pub enum FailureCode {
	#[error("Failed")]
	Failure = 0x0100, // 01xx (except 0107 and 0116
	#[error("No such attribute")]
	NoSuchAttribute = 0x0105,
	#[error("Invalid Attribute Value")]
	InvalidAttributeValue = 0x0106,
	#[error("Processing Failure")]
	ProcessingFailure = 0x0110,
	#[error("Duplicate SOP Instance")]
	DuplicateSOPInstance = 0x0111,
	#[error("No such SOP Instance")]
	NoSuchSOPInstance = 0x0112,
	#[error("No such Event Type")]
	NoSuchEvent = 0x0113,
	#[error("No such argument")]
	NoSuchArgument = 0x0114,
	#[error("Invalid argument value")]
	InvalidArgument = 0x0115,
	#[error("Invalid SOP Instance")]
	InvalidSOPInstance = 0x0117,
	#[error("No such SOP Class")]
	NoSuchSOPClass = 0x0118,
	#[error("Class-Instance conflict")]
	ClassInstance = 0x0119,
	#[error("Missing Attribute")]
	MissingAttribute = 0x0120,
	#[error("Missing Attribute Value")]
	MissingAttributeValue = 0x0121,
	#[error("SOP Class not supported")]
	InvalidSOPClass = 0x0122,
	#[error("No such Action Type")]
	NoSuchActionType = 0x0123,
	#[error("Not authorized")]
	NotAuthorized = 0x0124,
	#[error("Duplicate Message ID")]
	DuplicateOP = 0x0210,
	#[error("Unrecognized operation")]
	UnrecognizedOperation = 0x0211,
	#[error("Mistyped argument")]
	MistypedArgument = 0x0212,
	#[error("Resource limitation")]
	ResourceLimitation = 0x0213,
	#[error("Out of resources")]
	OutOfResourcesA = 0xA100,
	#[error("Move Destination unknown")]
	MoveDestination = 0xA201,
	#[error("Out of resources")]
	OutOfResources = 0xA700,
	#[error("Out of resources for Calculation")]
	OutOfResourcesCalc = 0xA701,
	#[error("Out of resources for operation")]
	OutOfResourcesOp = 0xA702,
	#[error("Data Set does not match SOP Class")]
	SOPClassDoesNotMatch = 0xA900, // 0xA9xx
	#[error("Cannot understand")]
	CannotUnderstand = 0xC000, // Cxxx
	#[error("Operation was cancelled")]
	Canceled = 0xfe00,
}

pub fn match_status(code:u16) -> Status
{
	match code {
		0x0000 => Ok(StatusOk::Success),
		0x0001 ..= 0x00FF => 
			StatusWarning::try_from(code).expect("invalid status code").into(),
		0xFF00 ..= 0xFFFF =>  
			Ok(StatusOk::Pending{succeeded: None,warn: None,fail: None,to_do: None,}),
		0xA700 ..= 0xA7ff | 0xA900 ..= 0xA9ff | 0xC000 ..= 0xCfff  => 
			FailureCode::try_from(code).expect("invalid status code").into(),
		_ => {
			error!("Unknown error code: {:04X}", code);
			Err(StatusFailure{code: FailureCode::CannotUnderstand,offending_element: None,comment: None})

		}
	}
}
pub fn get_status(stat:impl Into<Status>) -> u16 {
	match stat.into() {
		Ok(StatusOk::Success) => 0x0000,
		Ok(StatusOk::Pending{..}) => 0xff00,
		Ok(StatusOk::Warning{code, ..}) => code.into(),
		Err(StatusFailure{code, ..}) => code.into(),
	}
}
impl From<StatusOk> for Status{
	fn from(value: StatusOk) -> Self {Ok(value)}
}
impl From<StatusWarning> for Status{
	fn from(code: StatusWarning) -> Self
	{
		Ok(StatusOk::Warning{code, offending_element: None,	comment: None})
	}
}
impl From<FailureCode> for Status{
	fn from(code: FailureCode) -> Self 
	{
		Err(StatusFailure{code,	offending_element: None, comment: None})
	}
}

impl From<DimseError> for StatusFailure
{
	fn from(err: DimseError) -> Self
	{
		StatusFailure::new(FailureCode::Failure, Some(err.to_string()))
	}
}

impl Display for StatusFailure{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result
	{
		self.code.fmt(f)
	}
}

impl snafu::Error for StatusFailure
{}
