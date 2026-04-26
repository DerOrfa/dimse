
|  Symbol  | Description            |
|:--------:|------------------------|
|    U     | Unique Key Attribute   |
|    R     | Required Key Attribute |
|    O     | Optional Key Attribute |

# [Query/Retrieve Information Model Definition](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.2.html)
The Query/Retrieve Information Model is identified by the SOP Class negotiated at Association establishment time. The SOP Class is composed of both an Information Model and a DIMSE-C Service Group.
- This SOP Class identifies the class of the Query/Retrieve Information Model (i.e., not the SOP Class of the stored SOP Instances for which the SCP has information).
- [Matching](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.2.2.2.html)

## [Query Retrieve Service Class ](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/chapter_C.html)
- [Additional Query/Retrieve Attributes](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.3.4.html)

### C-FIND
- [Protocol](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.2.html)
- [Operation](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.html#sect_C.4.1)
- [Attribute Matching](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.2.2.2.html)
#### Message-Table
- [C-FIND](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.2.html#table_9.3-3)
- [C_FIND_RSP](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.2.2.html#table_9.3-4)
- [C-CANCEL-FIND-RQ](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.2.3.html#table_9.3-5)
#### Response status
https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.html#table_C.4-1

## C-MOVE
- [Operation](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.2.html#sect_C.4.2.1)
-

## C-GET
- [Parameters](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.html#sect_C.4.3.1)
- [Behavior](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.2.html)
- [Protocol](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.3.html)
### Response status
- https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.html#table_C.4-3

# Parameter Req/Rsp
- [C-GET](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-3)
- [C-MOVE](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-4)
- [C-ECHO](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-5)
- [C-STORE](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.html#table_9.3-1)
- [C-STORE-RSP](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.html#table_9.3-2)

## [Storage Service Class](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/chapter_B.html)
- [Standard SOP Classes](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.5.html)
### C-STORE
- [Service](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html)
- [Protocol](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.html)
- [Parameters](https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-1)
- [Related General SOP Classes](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.3.html#sect_B.3.1.4)
- [Conformance](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.4.html)
#### [SCU Fall-Back Behavior](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.4.2.html#sect_B.4.2.1)
During Association Negotiation, an application may propose a Specialized SOP Class and its Related General SOP Class in separate Presentation Contexts as a Storage SCU. If the Association Acceptor rejects the Specialized SOP Class Presentation Context, but accepts the Related General SOP Class Presentation Context, the application may send instances of the Specialized SOP Class as instances of the Related General SOP Class. In this fall-back behavior, the SOP Class UID of the instance shall be the UID of the Related General SOP Class, and any special semantics associated with the Specialized SOP Class may be lost; the SOP Instance UID shall remain the same.
#### Message-Table
- [C-STORE-RSP-Status](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_B.2.3.html#table_B.2-1)



- [SOP Classes](https://www.dicomlibrary.com/dicom/sop)
- [Transfer Syntaxes](https://www.dicomlibrary.com/dicom/transfer-syntax)