use dicom::object::InMemDicomObject;
use dicom_ul::pdu::PDataValue;
use std::path::PathBuf;

/// Attachment for Payload can be a file reference, a DICOM object or empty
#[derive(Debug, Default, PartialEq, Clone)]
pub enum Attachment {
	File{path:PathBuf,offset:u64,pc_id:u8},
	SmallObj(InMemDicomObject),
	Obj(InMemDicomObject),
	#[default]
	None
}

/// DIMSE payload to be sent to another peer
/// Consists of a DIMSE PDu data field that is a command and optionally an attachment
pub struct Payload {
	pub command:PDataValue,
	pub attachment: Attachment,
}
