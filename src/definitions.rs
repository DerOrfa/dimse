use dicom::dictionary_std::uids::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::{AsRefStr, Display};
use thiserror::Error;

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


#[derive(Debug, Default, Clone, Eq, PartialEq, TryFromPrimitive,IntoPrimitive,Error)]
#[repr(u16)]
pub enum WarningCode {
	#[error("Attribute List warning")]
	AttributeListWarning = 0x0107,
	#[error("Attribute Value out of range")]
	AttributeValueOutOfRange = 0x0116,
	#[error("Warning A")]
	#[default]
	Warning = 0x0001,
	#[error("Coercion of Data Elements")]
	DataCoercion = 0xB000, //bxxx
	#[error("Elements Discarded")]
	ElementsDiscarded = 0xB006,
	#[error("Data Set does not match SOP Class")]
	DataSetDoesNotMatch = 0xB007,
}

#[derive(Debug, Default, Clone, Eq, PartialEq, TryFromPrimitive,IntoPrimitive,Error)]
#[repr(u16)]
pub enum FailureCode {
	#[error("Failed")]
	#[default]
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
