use crate::msg::{MessageType, Response};
use crate::payload::Payload;
use dicom_ul::Pdu;
use snafu::prelude::*;
use tokio::sync::mpsc::error::SendError;
use crate::status::StatusFailure;

#[derive(Debug, Snafu)]
pub enum DimseError {
	#[snafu(whatever, display("{message}"))]
	GenericError {
		message: String,
		#[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
		source: Option<Box<dyn std::error::Error + Send + Sync>>,
	},
	#[snafu(display("Expected pdu data, but got {actual:?}"))]
	UnexpectedNonDataPdu{actual:Pdu},
	#[snafu(display("Unknown transfer syntax: {ts}"))]
	TransferSyntaxNotFound{ts:String},
	#[snafu(display("Unsupported transfer syntax: {uid}({name})"))]
	UnsupportedSyntax{uid:String, name:String},
	#[snafu(display("Unsupported presentation context {name}({uid})"))]
	UnsupportedPC{uid:String, name:String},
	#[snafu(display("Unknown SOP class {class}"))]
	UnknownSOPClass{class:String},
	#[snafu(display("Missing message id"))]
	NoMessageID,
	#[snafu(display("Expected message {expected}, but got {actual}"))] // @replace those below with this
	UnexpectedMessage{expected:String, actual:MessageType},
	#[snafu(display("Expected message type {expected}, but got {actual}"))]
	UnexpectedMessageType{expected:String, actual:String},
	#[snafu(display("Unexpected response {name}, don't know any related requests"))]
	UnexpectedResponse{name:String},
	#[snafu(display("Unexpected empty message"))]
	UnexpectedEmptyMsg,
	#[snafu(display("Incomplete message"))]
	IncompleteMsg,
	#[snafu(transparent)]
	AccessError{source:dicom::object::AccessError},
	#[snafu(transparent)]
	DcmReadError{source: dicom::object::ReadError},
	#[snafu(transparent)]
	DcmWriteError{source:dicom::object::WriteError},
	#[snafu(transparent)]
	AssReadError{source: dicom_ul::association::Error},
	#[snafu(display("Invalid status code 0x{code:04X}"))]
	InvalidStatus{code:u16},
	#[snafu(transparent)]
	StatusFailure{source:StatusFailure},
	#[snafu(transparent)]
	PayloadSendError{source:SendError<Payload>},
	#[snafu(transparent)]
	PipeSendError{source:SendError<Response>},
	#[snafu(transparent)]
	IoError{source: std::io::Error},
	#[snafu(transparent)]
	JoinError{source: tokio::task::JoinError},
}

pub type Result<T> = std::result::Result<T, DimseError>;
