use dicom::core::DataDictionary;
use crate::RetrieveLevel;
use dicom::core::header::{HasLength, Header};
use dicom::object::mem::InMemElement;
use dicom::object::{InMemDicomObject, Tag};
use dicom_dictionary_std::{tags, StandardDataDictionary};
use glob_match::glob_match;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct Identifier {
	pub filters: Vec<InMemElement>,
	lookup: Vec<InMemElement>,
	pub level: Option<RetrieveLevel>,
}

impl Identifier {
	pub fn extract(&self, obj: &mut InMemDicomObject) -> (Vec<InMemElement>, Vec<&InMemElement>) {
		let (ret, mut missing) = self.matches(&obj);
		let mut ret = ret.into_iter().map(InMemElement::clone).collect::<Vec<_>>();
		for look in &self.lookup {
			if let Some(found) = obj.take(look.tag())
				&& !found.is_empty()
			{
				ret.push(found);
			} else {
				missing.push(look);
			}
		}
		(ret, missing)
	}

	pub fn process_reply(&self, mut obj:InMemDicomObject) -> InMemDicomObject
	{
		let (extract,missing) = self.extract(&mut obj);
		let results = missing.into_iter().cloned().chain(extract);
		let mut obj = InMemDicomObject::from_element_iter(results);
		if let Some(level) = &self.level {
			obj.put_str(tags::QUERY_RETRIEVE_LEVEL, dicom::core::header::VR::CS, level.as_ref());
		}

		let dict = StandardDataDictionary::default();
		let tagname= move |tag|dict.by_tag(tag)
			.map(|e|format!("{}{}",e.alias,tag.to_string()))
			.unwrap_or(format!("{tag}"));
		for e in &obj
		{
			if e.is_empty(){debug!("Failed to answer request for {}", tagname(e.tag()));}
		}

		obj
	}
	/// Returns all filter-elements existing and matching in obj.
	/// Elements missing in obj are put into `.1`.
	/// Existing but mismatching elements are ignored.
	pub fn matches<'a>(&self, obj:&'a InMemDicomObject) -> (Vec<&'a InMemElement>, Vec<&InMemElement>) {
		let (found,missing) = self.matches_opt(obj);
		(
			found.into_iter().filter_map(|o|o).collect(),
			missing
		)
	}
	pub fn matches_opt<'a>(&self, obj:&'a InMemDicomObject) -> (Vec<Option<&'a InMemElement>>, Vec<&InMemElement>) {
		let mut missing = vec![];
		let mut found = vec![];
		for filter in &self.filters {
			if let Some(element) = obj.get(filter.tag()){
				let element_s = element.to_str().unwrap();
				let mask = filter.to_str().unwrap();
				found.push(glob_match(mask.as_ref(), element_s.as_ref()).then_some(element));
			} else { missing.push(filter); }
		}
		(found,missing)
	}
	/// returns true if all filter-elements exist in obj and match
	/// return false if filter-elements missing in obj
	pub fn matches_all(&self, obj: &InMemDicomObject) -> bool {
		let (found,missing) = self.matches_opt(obj);
		missing.is_empty() && found.iter().find(|e|e.is_none()).is_none()
	}

	/// returns the filter if this identifier has the tag in its filters
	pub fn contains(&self, tag: Tag) -> Option<&InMemElement> {
		self.filters.iter().find(|e|e.tag()==tag)
	}
}

impl<T> From<T> for Identifier where T:IntoIterator<Item=InMemElement>
{
	fn from(value: T) -> Self {
		let mut lookup = vec![];
		let mut filters = vec![];
		let mut level = None;
		const IGNORE:[Tag;3] = [tags::QUERY_RETRIEVE_VIEW, tags::SPECIFIC_CHARACTER_SET, tags::TIMEZONE_OFFSET_FROM_UTC]; // @todo actually should not be ignored

		for element in value.into_iter().filter(|e|!IGNORE.contains(&e.tag())) {
			if element.is_empty(){
				lookup.push(element);
			}
			else if element.tag() == tags::QUERY_RETRIEVE_LEVEL
			{
				let lvl = element.to_str().map_err(|e|e.to_string())
					.and_then(|lvl|RetrieveLevel::try_from(lvl.as_ref()).map_err(|e|e.to_string()));
				match lvl{
					Ok(lvl) => level = Some(lvl),
					Err(e) => warn!("Ignoring invalid QueryRetrieveLevel ({e})")
				}
			} else {
				filters.push(element);
			}
		}
		Self{filters,lookup, level }
	}
}