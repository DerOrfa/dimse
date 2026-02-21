use std::path::PathBuf;
use dicom::object::mem::InMemElement;
use dicom::object::InMemDicomObject;
use dicom_ul::pdu::PDataValue;

/// Attachment for Payload can be a file reference, a DICOM object or empty
#[derive(Debug, Default, PartialEq, Clone)]
pub enum Attachment {
	File{path:PathBuf,offset:u64,pc_id:u8},
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

impl Payload {
	pub fn new(command:PDataValue, attach:impl IntoIterator<Item=InMemElement>) -> Payload {
		let attachment = attach.into_iter().collect::<Vec<_>>();
		let attachment = if attachment.len() == 0 {
			Attachment::None
		} else {
			Attachment::Obj(InMemDicomObject::from_element_iter(attachment))
		};
		Payload {command, attachment}
	}
}

impl From<PDataValue> for Payload {
	fn from(command:PDataValue) -> Payload {
		Payload::new(command, vec![])
	}
}

