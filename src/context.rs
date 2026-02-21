use crate::error::DimseError::{TransferSyntaxNotFound, UnsupportedPC, UnsupportedSyntax};
use crate::error::Result;
use dicom::core::dictionary::UidDictionary;
use dicom::object::FileMetaTable;
use dicom::transfer_syntax::{TransferSyntax, TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_dictionary_std::StandardSopClassDictionary;
use dicom_ul::pdu::PresentationContextNegotiated;
use std::collections::HashMap;


#[derive(Debug, Clone, Default)]
pub struct Context{
	pcs:HashMap<u8,PresentationContextNegotiated>,
	reg:TransferSyntaxRegistry
}

impl Context
{
	pub fn new(cntx:&[PresentationContextNegotiated]) -> Self{
		let pcs= cntx.iter().map(|c|(c.id,c.clone())).collect();
		Context{ pcs, ..Default::default() }
	}
	pub fn select_pc(&self, pc_id:u8) -> &PresentationContextNegotiated{
		self.pcs.get(&pc_id).expect(format!("Invalid presentation context id: {}", pc_id).as_str())
	}
	pub fn select_ts(&self, pc_id:u8) -> Result<&TransferSyntax>
	{
		let ts = &self.select_pc(pc_id).transfer_syntax;
		self.reg.get(ts).ok_or(TransferSyntaxNotFound{ts:ts.clone()})
	}
	pub fn find_pc(&self, uid:impl ToString) -> Result<&PresentationContextNegotiated>
	{
		let uid = uid.to_string();
		match self.pcs.iter().find(|(_,ctx)|ctx.abstract_syntax == uid)
		{
			None => {
				let name = StandardSopClassDictionary::default().by_uid(&uid)
					.map_or("<unknown>",|a|a.name);
				Err(UnsupportedPC {uid,name:name.to_string()})
			}
			Some((_,pc)) => Ok(pc)
		}
	}
	pub fn find_ts(&self, uid:impl ToString) -> Result<&TransferSyntax>
	{
		let uid = uid.to_string();
		match self.pcs.iter().find(|(_,ctx)|ctx.transfer_syntax == uid)
		{
			None => {
				let name = self.reg.get(uid.as_str())
					.map_or("<unknown>",|s|s.name());
				Err(UnsupportedSyntax {uid,name:name.to_string()})
			}
			Some((id,_)) => self.select_ts(*id)
		}
	}
	pub fn match_file(&self, meta:&FileMetaTable) -> Option<&PresentationContextNegotiated>
	{
		let pc = meta.media_storage_sop_class_uid();
		let ts = meta.transfer_syntax();
		self.pcs.iter()
			.find(|(_,ctx)|ctx.transfer_syntax == ts && ctx.abstract_syntax == pc)
			.map(|(id,_)| self.select_pc(*id))

	}
}