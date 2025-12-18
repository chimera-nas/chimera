// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "smb_dump.h"
#include "smb_internal.h"

static const char *
smb_command_name(uint32_t command)
{
    switch (command) {
        case SMB2_NEGOTIATE:
            return "Negotiate";
        case SMB2_SESSION_SETUP:
            return "SessionSetup";
        case SMB2_LOGOFF:
            return "Logoff";
        case SMB2_TREE_CONNECT:
            return "TreeConnect";
        case SMB2_TREE_DISCONNECT:
            return "TreeDisconnect";
        case SMB2_CREATE:
            return "Create";
        case SMB2_CLOSE:
            return "Close";
        case SMB2_FLUSH:
            return "Flush";
        case SMB2_READ:
            return "Read";
        case SMB2_WRITE:
            return "Write";
        case SMB2_LOCK:
            return "Lock";
        case SMB2_IOCTL:
            return "Ioctl";
        case SMB2_CANCEL:
            return "Cancel";
        case SMB2_ECHO:
            return "Echo";
        case SMB2_QUERY_DIRECTORY:
            return "QueryDirectory";
        case SMB2_CHANGE_NOTIFY:
            return "ChangeNotify";
        case SMB2_QUERY_INFO:
            return "QueryInfo";
        case SMB2_SET_INFO:
            return "SetInfo";
        case SMB2_OPLOCK_BREAK:
            return "OplockBreak";
        case SMB1_NEGOTIATE:
            return "SMB1Negotiate";
        default:
            return "Unknown";
    } /* switch */
} /* smb_command_name */

static const char *
smb_status_name(uint32_t status)
{
    switch (status) {
        case SMB2_STATUS_SUCCESS:
            return "Success";
        case SMB2_STATUS_SHUTDOWN:
            return "Shutdown";
        case SMB2_STATUS_PENDING:
            return "Pending";
        case SMB2_STATUS_SMB_BAD_FID:
            return "SmbBadFid";
        case SMB2_STATUS_NO_MORE_FILES:
            return "NoMoreFiles";
        case SMB2_STATUS_UNSUCCESSFUL:
            return "Unsuccessful";
        case SMB2_STATUS_NOT_IMPLEMENTED:
            return "NotImplemented";
        case SMB2_STATUS_INVALID_INFO_CLASS:
            return "InvalidInfoClass";
        case SMB2_STATUS_INFO_LENGTH_MISMATCH:
            return "InfoLengthMismatch";
        case SMB2_STATUS_ACCESS_VIOLATION:
            return "AccessViolation";
        case SMB2_STATUS_IN_PAGE_ERROR:
            return "InPageError";
        case SMB2_STATUS_PAGEFILE_QUOTA:
            return "PagefileQuota";
        case SMB2_STATUS_INVALID_HANDLE:
            return "InvalidHandle";
        case SMB2_STATUS_BAD_INITIAL_STACK:
            return "BadInitialStack";
        case SMB2_STATUS_BAD_INITIAL_PC:
            return "BadInitialPc";
        case SMB2_STATUS_INVALID_CID:
            return "InvalidCid";
        case SMB2_STATUS_TIMER_NOT_CANCELED:
            return "TimerNotCanceled";
        case SMB2_STATUS_INVALID_PARAMETER:
            return "InvalidParameter";
        case SMB2_STATUS_NO_SUCH_DEVICE:
            return "NoSuchDevice";
        case SMB2_STATUS_NO_SUCH_FILE:
            return "NoSuchFile";
        case SMB2_STATUS_INVALID_DEVICE_REQUEST:
            return "InvalidDeviceRequest";
        case SMB2_STATUS_END_OF_FILE:
            return "EndOfFile";
        case SMB2_STATUS_WRONG_VOLUME:
            return "WrongVolume";
        case SMB2_STATUS_NO_MEDIA_IN_DEVICE:
            return "NoMediaInDevice";
        case SMB2_STATUS_UNRECOGNIZED_MEDIA:
            return "UnrecognizedMedia";
        case SMB2_STATUS_NONEXISTENT_SECTOR:
            return "NonexistentSector";
        case SMB2_STATUS_MORE_PROCESSING_REQUIRED:
            return "MoreProcessingRequired";
        case SMB2_STATUS_NO_MEMORY:
            return "NoMemory";
        case SMB2_STATUS_CONFLICTING_ADDRESSES:
            return "ConflictingAddresses";
        case SMB2_STATUS_NOT_MAPPED_VIEW:
            return "NotMappedView";
        case SMB2_STATUS_UNABLE_TO_FREE_VM:
            return "UnableToFreeVm";
        case SMB2_STATUS_UNABLE_TO_DELETE_SECTION:
            return "UnableToDeleteSection";
        case SMB2_STATUS_INVALID_SYSTEM_SERVICE:
            return "InvalidSystemService";
        case SMB2_STATUS_ILLEGAL_INSTRUCTION:
            return "IllegalInstruction";
        case SMB2_STATUS_INVALID_LOCK_SEQUENCE:
            return "InvalidLockSequence";
        case SMB2_STATUS_INVALID_VIEW_SIZE:
            return "InvalidViewSize";
        case SMB2_STATUS_INVALID_FILE_FOR_SECTION:
            return "InvalidFileForSection";
        case SMB2_STATUS_ALREADY_COMMITTED:
            return "AlreadyCommitted";
        case SMB2_STATUS_ACCESS_DENIED:
            return "AccessDenied";
        case SMB2_STATUS_BUFFER_TOO_SMALL:
            return "BufferTooSmall";
        case SMB2_STATUS_OBJECT_TYPE_MISMATCH:
            return "ObjectTypeMismatch";
        case SMB2_STATUS_NONCONTINUABLE_EXCEPTION:
            return "NoncontinuableException";
        case SMB2_STATUS_INVALID_DISPOSITION:
            return "InvalidDisposition";
        case SMB2_STATUS_UNWIND:
            return "Unwind";
        case SMB2_STATUS_BAD_STACK:
            return "BadStack";
        case SMB2_STATUS_INVALID_UNWIND_TARGET:
            return "InvalidUnwindTarget";
        case SMB2_STATUS_NOT_LOCKED:
            return "NotLocked";
        case SMB2_STATUS_PARITY_ERROR:
            return "ParityError";
        case SMB2_STATUS_UNABLE_TO_DECOMMIT_VM:
            return "UnableToDecommitVm";
        case SMB2_STATUS_NOT_COMMITTED:
            return "NotCommitted";
        case SMB2_STATUS_INVALID_PORT_ATTRIBUTES:
            return "InvalidPortAttributes";
        case SMB2_STATUS_PORT_MESSAGE_TOO_LONG:
            return "PortMessageTooLong";
        case SMB2_STATUS_INVALID_PARAMETER_MIX:
            return "InvalidParameterMix";
        case SMB2_STATUS_INVALID_QUOTA_LOWER:
            return "InvalidQuotaLower";
        case SMB2_STATUS_DISK_CORRUPT_ERROR:
            return "DiskCorruptError";
        case SMB2_STATUS_OBJECT_NAME_INVALID:
            return "ObjectNameInvalid";
        case SMB2_STATUS_OBJECT_NAME_NOT_FOUND:
            return "ObjectNameNotFound";
        case SMB2_STATUS_OBJECT_NAME_COLLISION:
            return "ObjectNameCollision";
        case SMB2_STATUS_HANDLE_NOT_WAITABLE:
            return "HandleNotWaitable";
        case SMB2_STATUS_PORT_DISCONNECTED:
            return "PortDisconnected";
        case SMB2_STATUS_DEVICE_ALREADY_ATTACHED:
            return "DeviceAlreadyAttached";
        case SMB2_STATUS_OBJECT_PATH_INVALID:
            return "ObjectPathInvalid";
        case SMB2_STATUS_OBJECT_PATH_NOT_FOUND:
            return "ObjectPathNotFound";
        case SMB2_STATUS_OBJECT_PATH_SYNTAX_BAD:
            return "ObjectPathSyntaxBad";
        case SMB2_STATUS_DATA_OVERRUN:
            return "DataOverrun";
        case SMB2_STATUS_DATA_LATE_ERROR:
            return "DataLateError";
        case SMB2_STATUS_DATA_ERROR:
            return "DataError";
        case SMB2_STATUS_CRC_ERROR:
            return "CrcError";
        case SMB2_STATUS_SECTION_TOO_BIG:
            return "SectionTooBig";
        case SMB2_STATUS_PORT_CONNECTION_REFUSED:
            return "PortConnectionRefused";
        case SMB2_STATUS_INVALID_PORT_HANDLE:
            return "InvalidPortHandle";
        case SMB2_STATUS_SHARING_VIOLATION:
            return "SharingViolation";
        case SMB2_STATUS_QUOTA_EXCEEDED:
            return "QuotaExceeded";
        case SMB2_STATUS_INVALID_PAGE_PROTECTION:
            return "InvalidPageProtection";
        case SMB2_STATUS_MUTANT_NOT_OWNED:
            return "MutantNotOwned";
        case SMB2_STATUS_SEMAPHORE_LIMIT_EXCEEDED:
            return "SemaphoreLimitExceeded";
        case SMB2_STATUS_PORT_ALREADY_SET:
            return "PortAlreadySet";
        case SMB2_STATUS_SECTION_NOT_IMAGE:
            return "SectionNotImage";
        case SMB2_STATUS_SUSPEND_COUNT_EXCEEDED:
            return "SuspendCountExceeded";
        case SMB2_STATUS_THREAD_IS_TERMINATING:
            return "ThreadIsTerminating";
        case SMB2_STATUS_BAD_WORKING_SET_LIMIT:
            return "BadWorkingSetLimit";
        case SMB2_STATUS_INCOMPATIBLE_FILE_MAP:
            return "IncompatibleFileMap";
        case SMB2_STATUS_SECTION_PROTECTION:
            return "SectionProtection";
        case SMB2_STATUS_EAS_NOT_SUPPORTED:
            return "EasNotSupported";
        case SMB2_STATUS_EA_TOO_LARGE:
            return "EaTooLarge";
        case SMB2_STATUS_NONEXISTENT_EA_ENTRY:
            return "NonexistentEaEntry";
        case SMB2_STATUS_NO_EAS_ON_FILE:
            return "NoEasOnFile";
        case SMB2_STATUS_EA_CORRUPT_ERROR:
            return "EaCorruptError";
        case SMB2_STATUS_FILE_LOCK_CONFLICT:
            return "FileLockConflict";
        case SMB2_STATUS_LOCK_NOT_GRANTED:
            return "LockNotGranted";
        case SMB2_STATUS_DELETE_PENDING:
            return "DeletePending";
        case SMB2_STATUS_CTL_FILE_NOT_SUPPORTED:
            return "CtlFileNotSupported";
        case SMB2_STATUS_UNKNOWN_REVISION:
            return "UnknownRevision";
        case SMB2_STATUS_REVISION_MISMATCH:
            return "RevisionMismatch";
        case SMB2_STATUS_INVALID_OWNER:
            return "InvalidOwner";
        case SMB2_STATUS_INVALID_PRIMARY_GROUP:
            return "InvalidPrimaryGroup";
        case SMB2_STATUS_NO_IMPERSONATION_TOKEN:
            return "NoImpersonationToken";
        case SMB2_STATUS_CANT_DISABLE_MANDATORY:
            return "CantDisableMandatory";
        case SMB2_STATUS_NO_LOGON_SERVERS:
            return "NoLogonServers";
        case SMB2_STATUS_NO_SUCH_LOGON_SESSION:
            return "NoSuchLogonSession";
        case SMB2_STATUS_NO_SUCH_PRIVILEGE:
            return "NoSuchPrivilege";
        case SMB2_STATUS_PRIVILEGE_NOT_HELD:
            return "PrivilegeNotHeld";
        case SMB2_STATUS_INVALID_ACCOUNT_NAME:
            return "InvalidAccountName";
        case SMB2_STATUS_USER_EXISTS:
            return "UserExists";
        case SMB2_STATUS_NO_SUCH_USER:
            return "NoSuchUser";
        case SMB2_STATUS_GROUP_EXISTS:
            return "GroupExists";
        case SMB2_STATUS_NO_SUCH_GROUP:
            return "NoSuchGroup";
        case SMB2_STATUS_MEMBER_IN_GROUP:
            return "MemberInGroup";
        case SMB2_STATUS_MEMBER_NOT_IN_GROUP:
            return "MemberNotInGroup";
        case SMB2_STATUS_LAST_ADMIN:
            return "LastAdmin";
        case SMB2_STATUS_WRONG_PASSWORD:
            return "WrongPassword";
        case SMB2_STATUS_ILL_FORMED_PASSWORD:
            return "IllFormedPassword";
        case SMB2_STATUS_PASSWORD_RESTRICTION:
            return "PasswordRestriction";
        case SMB2_STATUS_LOGON_FAILURE:
            return "LogonFailure";
        case SMB2_STATUS_ACCOUNT_RESTRICTION:
            return "AccountRestriction";
        case SMB2_STATUS_INVALID_LOGON_HOURS:
            return "InvalidLogonHours";
        case SMB2_STATUS_INVALID_WORKSTATION:
            return "InvalidWorkstation";
        case SMB2_STATUS_PASSWORD_EXPIRED:
            return "PasswordExpired";
        case SMB2_STATUS_ACCOUNT_DISABLED:
            return "AccountDisabled";
        case SMB2_STATUS_NONE_MAPPED:
            return "NoneMapped";
        case SMB2_STATUS_TOO_MANY_LUIDS_REQUESTED:
            return "TooManyLuidsRequested";
        case SMB2_STATUS_LUIDS_EXHAUSTED:
            return "LuidsExhausted";
        case SMB2_STATUS_INVALID_SUB_AUTHORITY:
            return "InvalidSubAuthority";
        case SMB2_STATUS_INVALID_ACL:
            return "InvalidAcl";
        case SMB2_STATUS_INVALID_SID:
            return "InvalidSid";
        case SMB2_STATUS_INVALID_SECURITY_DESCR:
            return "InvalidSecurityDescr";
        case SMB2_STATUS_PROCEDURE_NOT_FOUND:
            return "ProcedureNotFound";
        case SMB2_STATUS_INVALID_IMAGE_FORMAT:
            return "InvalidImageFormat";
        case SMB2_STATUS_NO_TOKEN:
            return "NoToken";
        case SMB2_STATUS_BAD_INHERITANCE_ACL:
            return "BadInheritanceAcl";
        case SMB2_STATUS_RANGE_NOT_LOCKED:
            return "RangeNotLocked";
        case SMB2_STATUS_DISK_FULL:
            return "DiskFull";
        case SMB2_STATUS_SERVER_DISABLED:
            return "ServerDisabled";
        case SMB2_STATUS_SERVER_NOT_DISABLED:
            return "ServerNotDisabled";
        case SMB2_STATUS_TOO_MANY_GUIDS_REQUESTED:
            return "TooManyGuidsRequested";
        case SMB2_STATUS_INVALID_ID_AUTHORITY:
            return "InvalidIdAuthority";
        case SMB2_STATUS_AGENTS_EXHAUSTED:
            return "AgentsExhausted";
        case SMB2_STATUS_INVALID_VOLUME_LABEL:
            return "InvalidVolumeLabel";
        case SMB2_STATUS_SECTION_NOT_EXTENDED:
            return "SectionNotExtended";
        case SMB2_STATUS_NOT_MAPPED_DATA:
            return "NotMappedData";
        case SMB2_STATUS_RESOURCE_DATA_NOT_FOUND:
            return "ResourceDataNotFound";
        case SMB2_STATUS_RESOURCE_TYPE_NOT_FOUND:
            return "ResourceTypeNotFound";
        case SMB2_STATUS_RESOURCE_NAME_NOT_FOUND:
            return "ResourceNameNotFound";
        case SMB2_STATUS_ARRAY_BOUNDS_EXCEEDED:
            return "ArrayBoundsExceeded";
        case SMB2_STATUS_FLOAT_DENORMAL_OPERAND:
            return "FloatDenormalOperand";
        case SMB2_STATUS_FLOAT_DIVIDE_BY_ZERO:
            return "FloatDivideByZero";
        case SMB2_STATUS_FLOAT_INEXACT_RESULT:
            return "FloatInexactResult";
        case SMB2_STATUS_FLOAT_INVALID_OPERATION:
            return "FloatInvalidOperation";
        case SMB2_STATUS_FLOAT_OVERFLOW:
            return "FloatOverflow";
        case SMB2_STATUS_FLOAT_STACK_CHECK:
            return "FloatStackCheck";
        case SMB2_STATUS_FLOAT_UNDERFLOW:
            return "FloatUnderflow";
        case SMB2_STATUS_INTEGER_DIVIDE_BY_ZERO:
            return "IntegerDivideByZero";
        case SMB2_STATUS_INTEGER_OVERFLOW:
            return "IntegerOverflow";
        case SMB2_STATUS_PRIVILEGED_INSTRUCTION:
            return "PrivilegedInstruction";
        case SMB2_STATUS_TOO_MANY_PAGING_FILES:
            return "TooManyPagingFiles";
        case SMB2_STATUS_FILE_INVALID:
            return "FileInvalid";
        case SMB2_STATUS_ALLOTTED_SPACE_EXCEEDED:
            return "AllottedSpaceExceeded";
        case SMB2_STATUS_INSUFFICIENT_RESOURCES:
            return "InsufficientResources";
        case SMB2_STATUS_DFS_EXIT_PATH_FOUND:
            return "DfsExitPathFound";
        case SMB2_STATUS_DEVICE_DATA_ERROR:
            return "DeviceDataError";
        case SMB2_STATUS_DEVICE_NOT_CONNECTED:
            return "DeviceNotConnected";
        case SMB2_STATUS_DEVICE_POWER_FAILURE:
            return "DevicePowerFailure";
        case SMB2_STATUS_FREE_VM_NOT_AT_BASE:
            return "FreeVmNotAtBase";
        case SMB2_STATUS_MEMORY_NOT_ALLOCATED:
            return "MemoryNotAllocated";
        case SMB2_STATUS_WORKING_SET_QUOTA:
            return "WorkingSetQuota";
        case SMB2_STATUS_MEDIA_WRITE_PROTECTED:
            return "MediaWriteProtected";
        case SMB2_STATUS_DEVICE_NOT_READY:
            return "DeviceNotReady";
        case SMB2_STATUS_INVALID_GROUP_ATTRIBUTES:
            return "InvalidGroupAttributes";
        case SMB2_STATUS_BAD_IMPERSONATION_LEVEL:
            return "BadImpersonationLevel";
        case SMB2_STATUS_CANT_OPEN_ANONYMOUS:
            return "CantOpenAnonymous";
        case SMB2_STATUS_BAD_VALIDATION_CLASS:
            return "BadValidationClass";
        case SMB2_STATUS_BAD_TOKEN_TYPE:
            return "BadTokenType";
        case SMB2_STATUS_BAD_MASTER_BOOT_RECORD:
            return "BadMasterBootRecord";
        case SMB2_STATUS_INSTRUCTION_MISALIGNMENT:
            return "InstructionMisalignment";
        case SMB2_STATUS_INSTANCE_NOT_AVAILABLE:
            return "InstanceNotAvailable";
        case SMB2_STATUS_PIPE_NOT_AVAILABLE:
            return "PipeNotAvailable";
        case SMB2_STATUS_INVALID_PIPE_STATE:
            return "InvalidPipeState";
        case SMB2_STATUS_PIPE_BUSY:
            return "PipeBusy";
        case SMB2_STATUS_ILLEGAL_FUNCTION:
            return "IllegalFunction";
        case SMB2_STATUS_PIPE_DISCONNECTED:
            return "PipeDisconnected";
        case SMB2_STATUS_PIPE_CLOSING:
            return "PipeClosing";
        case SMB2_STATUS_PIPE_CONNECTED:
            return "PipeConnected";
        case SMB2_STATUS_PIPE_LISTENING:
            return "PipeListening";
        case SMB2_STATUS_INVALID_READ_MODE:
            return "InvalidReadMode";
        case SMB2_STATUS_IO_TIMEOUT:
            return "IoTimeout";
        case SMB2_STATUS_FILE_FORCED_CLOSED:
            return "FileForcedClosed";
        case SMB2_STATUS_PROFILING_NOT_STARTED:
            return "ProfilingNotStarted";
        case SMB2_STATUS_PROFILING_NOT_STOPPED:
            return "ProfilingNotStopped";
        case SMB2_STATUS_COULD_NOT_INTERPRET:
            return "CouldNotInterpret";
        case SMB2_STATUS_FILE_IS_A_DIRECTORY:
            return "FileIsADirectory";
        case SMB2_STATUS_NOT_SUPPORTED:
            return "NotSupported";
        case SMB2_STATUS_REMOTE_NOT_LISTENING:
            return "RemoteNotListening";
        case SMB2_STATUS_DUPLICATE_NAME:
            return "DuplicateName";
        case SMB2_STATUS_BAD_NETWORK_PATH:
            return "BadNetworkPath";
        case SMB2_STATUS_NETWORK_BUSY:
            return "NetworkBusy";
        case SMB2_STATUS_DEVICE_DOES_NOT_EXIST:
            return "DeviceDoesNotExist";
        case SMB2_STATUS_TOO_MANY_COMMANDS:
            return "TooManyCommands";
        case SMB2_STATUS_ADAPTER_HARDWARE_ERROR:
            return "AdapterHardwareError";
        case SMB2_STATUS_INVALID_NETWORK_RESPONSE:
            return "InvalidNetworkResponse";
        case SMB2_STATUS_UNEXPECTED_NETWORK_ERROR:
            return "UnexpectedNetworkError";
        case SMB2_STATUS_BAD_REMOTE_ADAPTER:
            return "BadRemoteAdapter";
        case SMB2_STATUS_PRINT_QUEUE_FULL:
            return "PrintQueueFull";
        case SMB2_STATUS_NO_SPOOL_SPACE:
            return "NoSpoolSpace";
        case SMB2_STATUS_PRINT_CANCELLED:
            return "PrintCancelled";
        case SMB2_STATUS_NETWORK_NAME_DELETED:
            return "NetworkNameDeleted";
        case SMB2_STATUS_NETWORK_ACCESS_DENIED:
            return "NetworkAccessDenied";
        case SMB2_STATUS_BAD_DEVICE_TYPE:
            return "BadDeviceType";
        case SMB2_STATUS_BAD_NETWORK_NAME:
            return "BadNetworkName";
        case SMB2_STATUS_TOO_MANY_NAMES:
            return "TooManyNames";
        case SMB2_STATUS_TOO_MANY_SESSIONS:
            return "TooManySessions";
        case SMB2_STATUS_SHARING_PAUSED:
            return "SharingPaused";
        case SMB2_STATUS_REQUEST_NOT_ACCEPTED:
            return "RequestNotAccepted";
        case SMB2_STATUS_REDIRECTOR_PAUSED:
            return "RedirectorPaused";
        case SMB2_STATUS_NET_WRITE_FAULT:
            return "NetWriteFault";
        case SMB2_STATUS_PROFILING_AT_LIMIT:
            return "ProfilingAtLimit";
        case SMB2_STATUS_NOT_SAME_DEVICE:
            return "NotSameDevice";
        case SMB2_STATUS_FILE_RENAMED:
            return "FileRenamed";
        case SMB2_STATUS_VIRTUAL_CIRCUIT_CLOSED:
            return "VirtualCircuitClosed";
        case SMB2_STATUS_NO_SECURITY_ON_OBJECT:
            return "NoSecurityOnObject";
        case SMB2_STATUS_CANT_WAIT:
            return "CantWait";
        case SMB2_STATUS_PIPE_EMPTY:
            return "PipeEmpty";
        case SMB2_STATUS_CANT_ACCESS_DOMAIN_INFO:
            return "CantAccessDomainInfo";
        case SMB2_STATUS_CANT_TERMINATE_SELF:
            return "CantTerminateSelf";
        case SMB2_STATUS_INVALID_SERVER_STATE:
            return "InvalidServerState";
        case SMB2_STATUS_INVALID_DOMAIN_STATE:
            return "InvalidDomainState";
        case SMB2_STATUS_INVALID_DOMAIN_ROLE:
            return "InvalidDomainRole";
        case SMB2_STATUS_NO_SUCH_DOMAIN:
            return "NoSuchDomain";
        case SMB2_STATUS_DOMAIN_EXISTS:
            return "DomainExists";
        case SMB2_STATUS_DOMAIN_LIMIT_EXCEEDED:
            return "DomainLimitExceeded";
        case SMB2_STATUS_OPLOCK_NOT_GRANTED:
            return "OplockNotGranted";
        case SMB2_STATUS_INVALID_OPLOCK_PROTOCOL:
            return "InvalidOplockProtocol";
        case SMB2_STATUS_INTERNAL_DB_CORRUPTION:
            return "InternalDbCorruption";
        case SMB2_STATUS_INTERNAL_ERROR:
            return "InternalError";
        case SMB2_STATUS_GENERIC_NOT_MAPPED:
            return "GenericNotMapped";
        case SMB2_STATUS_BAD_DESCRIPTOR_FORMAT:
            return "BadDescriptorFormat";
        case SMB2_STATUS_INVALID_USER_BUFFER:
            return "InvalidUserBuffer";
        case SMB2_STATUS_UNEXPECTED_IO_ERROR:
            return "UnexpectedIoError";
        case SMB2_STATUS_UNEXPECTED_MM_CREATE_ERR:
            return "UnexpectedMmCreateErr";
        case SMB2_STATUS_UNEXPECTED_MM_MAP_ERROR:
            return "UnexpectedMmMapError";
        case SMB2_STATUS_UNEXPECTED_MM_EXTEND_ERR:
            return "UnexpectedMmExtendErr";
        case SMB2_STATUS_NOT_LOGON_PROCESS:
            return "NotLogonProcess";
        case SMB2_STATUS_LOGON_SESSION_EXISTS:
            return "LogonSessionExists";
        case SMB2_STATUS_INVALID_PARAMETER_1:
            return "InvalidParameter1";
        case SMB2_STATUS_INVALID_PARAMETER_2:
            return "InvalidParameter2";
        case SMB2_STATUS_INVALID_PARAMETER_3:
            return "InvalidParameter3";
        case SMB2_STATUS_INVALID_PARAMETER_4:
            return "InvalidParameter4";
        case SMB2_STATUS_INVALID_PARAMETER_5:
            return "InvalidParameter5";
        case SMB2_STATUS_INVALID_PARAMETER_6:
            return "InvalidParameter6";
        case SMB2_STATUS_INVALID_PARAMETER_7:
            return "InvalidParameter7";
        case SMB2_STATUS_INVALID_PARAMETER_8:
            return "InvalidParameter8";
        case SMB2_STATUS_INVALID_PARAMETER_9:
            return "InvalidParameter9";
        case SMB2_STATUS_INVALID_PARAMETER_10:
            return "InvalidParameter10";
        case SMB2_STATUS_INVALID_PARAMETER_11:
            return "InvalidParameter11";
        case SMB2_STATUS_INVALID_PARAMETER_12:
            return "InvalidParameter12";
        case SMB2_STATUS_REDIRECTOR_NOT_STARTED:
            return "RedirectorNotStarted";
        case SMB2_STATUS_REDIRECTOR_STARTED:
            return "RedirectorStarted";
        case SMB2_STATUS_STACK_OVERFLOW:
            return "StackOverflow";
        case SMB2_STATUS_NO_SUCH_PACKAGE:
            return "NoSuchPackage";
        case SMB2_STATUS_BAD_FUNCTION_TABLE:
            return "BadFunctionTable";
        case SMB2_STATUS_DIRECTORY_NOT_EMPTY:
            return "DirectoryNotEmpty";
        case SMB2_STATUS_FILE_CORRUPT_ERROR:
            return "FileCorruptError";
        case SMB2_STATUS_NOT_A_DIRECTORY:
            return "NotADirectory";
        case SMB2_STATUS_BAD_LOGON_SESSION_STATE:
            return "BadLogonSessionState";
        case SMB2_STATUS_LOGON_SESSION_COLLISION:
            return "LogonSessionCollision";
        case SMB2_STATUS_NAME_TOO_LONG:
            return "NameTooLong";
        case SMB2_STATUS_FILES_OPEN:
            return "FilesOpen";
        case SMB2_STATUS_CONNECTION_IN_USE:
            return "ConnectionInUse";
        case SMB2_STATUS_MESSAGE_NOT_FOUND:
            return "MessageNotFound";
        case SMB2_STATUS_PROCESS_IS_TERMINATING:
            return "ProcessIsTerminating";
        case SMB2_STATUS_INVALID_LOGON_TYPE:
            return "InvalidLogonType";
        case SMB2_STATUS_NO_GUID_TRANSLATION:
            return "NoGuidTranslation";
        case SMB2_STATUS_CANNOT_IMPERSONATE:
            return "CannotImpersonate";
        case SMB2_STATUS_IMAGE_ALREADY_LOADED:
            return "ImageAlreadyLoaded";
        case SMB2_STATUS_ABIOS_NOT_PRESENT:
            return "AbiosNotPresent";
        case SMB2_STATUS_ABIOS_LID_NOT_EXIST:
            return "AbiosLidNotExist";
        case SMB2_STATUS_ABIOS_LID_ALREADY_OWNED:
            return "AbiosLidAlreadyOwned";
        case SMB2_STATUS_ABIOS_NOT_LID_OWNER:
            return "AbiosNotLidOwner";
        case SMB2_STATUS_ABIOS_INVALID_COMMAND:
            return "AbiosInvalidCommand";
        case SMB2_STATUS_ABIOS_INVALID_LID:
            return "AbiosInvalidLid";
        case SMB2_STATUS_ABIOS_SELECTOR_NOT_AVAILABLE:
            return "AbiosSelectorNotAvailable";
        case SMB2_STATUS_ABIOS_INVALID_SELECTOR:
            return "AbiosInvalidSelector";
        case SMB2_STATUS_NO_LDT:
            return "NoLdt";
        case SMB2_STATUS_INVALID_LDT_SIZE:
            return "InvalidLdtSize";
        case SMB2_STATUS_INVALID_LDT_OFFSET:
            return "InvalidLdtOffset";
        case SMB2_STATUS_INVALID_LDT_DESCRIPTOR:
            return "InvalidLdtDescriptor";
        case SMB2_STATUS_INVALID_IMAGE_NE_FORMAT:
            return "InvalidImageNeFormat";
        case SMB2_STATUS_RXACT_INVALID_STATE:
            return "RxactInvalidState";
        case SMB2_STATUS_RXACT_COMMIT_FAILURE:
            return "RxactCommitFailure";
        case SMB2_STATUS_MAPPED_FILE_SIZE_ZERO:
            return "MappedFileSizeZero";
        case SMB2_STATUS_TOO_MANY_OPENED_FILES:
            return "TooManyOpenedFiles";
        case SMB2_STATUS_CANCELLED:
            return "Cancelled";
        case SMB2_STATUS_CANNOT_DELETE:
            return "CannotDelete";
        case SMB2_STATUS_INVALID_COMPUTER_NAME:
            return "InvalidComputerName";
        case SMB2_STATUS_FILE_DELETED:
            return "FileDeleted";
        case SMB2_STATUS_SPECIAL_ACCOUNT:
            return "SpecialAccount";
        case SMB2_STATUS_SPECIAL_GROUP:
            return "SpecialGroup";
        case SMB2_STATUS_SPECIAL_USER:
            return "SpecialUser";
        case SMB2_STATUS_MEMBERS_PRIMARY_GROUP:
            return "MembersPrimaryGroup";
        case SMB2_STATUS_FILE_CLOSED:
            return "FileClosed";
        case SMB2_STATUS_TOO_MANY_THREADS:
            return "TooManyThreads";
        case SMB2_STATUS_THREAD_NOT_IN_PROCESS:
            return "ThreadNotInProcess";
        case SMB2_STATUS_TOKEN_ALREADY_IN_USE:
            return "TokenAlreadyInUse";
        case SMB2_STATUS_PAGEFILE_QUOTA_EXCEEDED:
            return "PagefileQuotaExceeded";
        case SMB2_STATUS_COMMITMENT_LIMIT:
            return "CommitmentLimit";
        case SMB2_STATUS_INVALID_IMAGE_LE_FORMAT:
            return "InvalidImageLeFormat";
        case SMB2_STATUS_INVALID_IMAGE_NOT_MZ:
            return "InvalidImageNotMz";
        case SMB2_STATUS_INVALID_IMAGE_PROTECT:
            return "InvalidImageProtect";
        case SMB2_STATUS_INVALID_IMAGE_WIN_16:
            return "InvalidImageWin16";
        case SMB2_STATUS_LOGON_SERVER_CONFLICT:
            return "LogonServerConflict";
        case SMB2_STATUS_TIME_DIFFERENCE_AT_DC:
            return "TimeDifferenceAtDc";
        case SMB2_STATUS_SYNCHRONIZATION_REQUIRED:
            return "SynchronizationRequired";
        case SMB2_STATUS_DLL_NOT_FOUND:
            return "DllNotFound";
        case SMB2_STATUS_OPEN_FAILED:
            return "OpenFailed";
        case SMB2_STATUS_IO_PRIVILEGE_FAILED:
            return "IoPrivilegeFailed";
        case SMB2_STATUS_ORDINAL_NOT_FOUND:
            return "OrdinalNotFound";
        case SMB2_STATUS_ENTRYPOINT_NOT_FOUND:
            return "EntrypointNotFound";
        case SMB2_STATUS_CONTROL_C_EXIT:
            return "ControlCExit";
        case SMB2_STATUS_LOCAL_DISCONNECT:
            return "LocalDisconnect";
        case SMB2_STATUS_REMOTE_DISCONNECT:
            return "RemoteDisconnect";
        case SMB2_STATUS_REMOTE_RESOURCES:
            return "RemoteResources";
        case SMB2_STATUS_LINK_FAILED:
            return "LinkFailed";
        case SMB2_STATUS_LINK_TIMEOUT:
            return "LinkTimeout";
        case SMB2_STATUS_INVALID_CONNECTION:
            return "InvalidConnection";
        case SMB2_STATUS_INVALID_ADDRESS:
            return "InvalidAddress";
        case SMB2_STATUS_DLL_INIT_FAILED:
            return "DllInitFailed";
        case SMB2_STATUS_MISSING_SYSTEMFILE:
            return "MissingSystemfile";
        case SMB2_STATUS_UNHANDLED_EXCEPTION:
            return "UnhandledException";
        case SMB2_STATUS_APP_INIT_FAILURE:
            return "AppInitFailure";
        case SMB2_STATUS_PAGEFILE_CREATE_FAILED:
            return "PagefileCreateFailed";
        case SMB2_STATUS_NO_PAGEFILE:
            return "NoPagefile";
        case SMB2_STATUS_INVALID_LEVEL:
            return "InvalidLevel";
        case SMB2_STATUS_WRONG_PASSWORD_CORE:
            return "WrongPasswordCore";
        case SMB2_STATUS_ILLEGAL_FLOAT_CONTEXT:
            return "IllegalFloatContext";
        case SMB2_STATUS_PIPE_BROKEN:
            return "PipeBroken";
        case SMB2_STATUS_REGISTRY_CORRUPT:
            return "RegistryCorrupt";
        case SMB2_STATUS_REGISTRY_IO_FAILED:
            return "RegistryIoFailed";
        case SMB2_STATUS_NO_EVENT_PAIR:
            return "NoEventPair";
        case SMB2_STATUS_UNRECOGNIZED_VOLUME:
            return "UnrecognizedVolume";
        case SMB2_STATUS_SERIAL_NO_DEVICE_INITED:
            return "SerialNoDeviceInited";
        case SMB2_STATUS_NO_SUCH_ALIAS:
            return "NoSuchAlias";
        case SMB2_STATUS_MEMBER_NOT_IN_ALIAS:
            return "MemberNotInAlias";
        case SMB2_STATUS_MEMBER_IN_ALIAS:
            return "MemberInAlias";
        case SMB2_STATUS_ALIAS_EXISTS:
            return "AliasExists";
        case SMB2_STATUS_LOGON_NOT_GRANTED:
            return "LogonNotGranted";
        case SMB2_STATUS_TOO_MANY_SECRETS:
            return "TooManySecrets";
        case SMB2_STATUS_SECRET_TOO_LONG:
            return "SecretTooLong";
        case SMB2_STATUS_INTERNAL_DB_ERROR:
            return "InternalDbError";
        case SMB2_STATUS_FULLSCREEN_MODE:
            return "FullscreenMode";
        case SMB2_STATUS_TOO_MANY_CONTEXT_IDS:
            return "TooManyContextIds";
        case SMB2_STATUS_LOGON_TYPE_NOT_GRANTED:
            return "LogonTypeNotGranted";
        case SMB2_STATUS_NOT_REGISTRY_FILE:
            return "NotRegistryFile";
        case SMB2_STATUS_NT_CROSS_ENCRYPTION_REQUIRED:
            return "NtCrossEncryptionRequired";
        case SMB2_STATUS_DOMAIN_CTRLR_CONFIG_ERROR:
            return "DomainCtrlrConfigError";
        case SMB2_STATUS_FT_MISSING_MEMBER:
            return "FtMissingMember";
        case SMB2_STATUS_ILL_FORMED_SERVICE_ENTRY:
            return "IllFormedServiceEntry";
        case SMB2_STATUS_ILLEGAL_CHARACTER:
            return "IllegalCharacter";
        case SMB2_STATUS_UNMAPPABLE_CHARACTER:
            return "UnmappableCharacter";
        case SMB2_STATUS_UNDEFINED_CHARACTER:
            return "UndefinedCharacter";
        case SMB2_STATUS_FLOPPY_VOLUME:
            return "FloppyVolume";
        case SMB2_STATUS_FLOPPY_ID_MARK_NOT_FOUND:
            return "FloppyIdMarkNotFound";
        case SMB2_STATUS_FLOPPY_WRONG_CYLINDER:
            return "FloppyWrongCylinder";
        case SMB2_STATUS_FLOPPY_UNKNOWN_ERROR:
            return "FloppyUnknownError";
        case SMB2_STATUS_FLOPPY_BAD_REGISTERS:
            return "FloppyBadRegisters";
        case SMB2_STATUS_DISK_RECALIBRATE_FAILED:
            return "DiskRecalibrateFailed";
        case SMB2_STATUS_DISK_OPERATION_FAILED:
            return "DiskOperationFailed";
        case SMB2_STATUS_DISK_RESET_FAILED:
            return "DiskResetFailed";
        case SMB2_STATUS_SHARED_IRQ_BUSY:
            return "SharedIrqBusy";
        case SMB2_STATUS_FT_ORPHANING:
            return "FtOrphaning";
        case SMB2_STATUS_PARTITION_FAILURE:
            return "PartitionFailure";
        case SMB2_STATUS_INVALID_BLOCK_LENGTH:
            return "InvalidBlockLength";
        case SMB2_STATUS_DEVICE_NOT_PARTITIONED:
            return "DeviceNotPartitioned";
        case SMB2_STATUS_UNABLE_TO_LOCK_MEDIA:
            return "UnableToLockMedia";
        case SMB2_STATUS_UNABLE_TO_UNLOAD_MEDIA:
            return "UnableToUnloadMedia";
        case SMB2_STATUS_EOM_OVERFLOW:
            return "EomOverflow";
        case SMB2_STATUS_NO_MEDIA:
            return "NoMedia";
        case SMB2_STATUS_NO_SUCH_MEMBER:
            return "NoSuchMember";
        case SMB2_STATUS_INVALID_MEMBER:
            return "InvalidMember";
        case SMB2_STATUS_KEY_DELETED:
            return "KeyDeleted";
        case SMB2_STATUS_NO_LOG_SPACE:
            return "NoLogSpace";
        case SMB2_STATUS_TOO_MANY_SIDS:
            return "TooManySids";
        case SMB2_STATUS_LM_CROSS_ENCRYPTION_REQUIRED:
            return "LmCrossEncryptionRequired";
        case SMB2_STATUS_KEY_HAS_CHILDREN:
            return "KeyHasChildren";
        case SMB2_STATUS_CHILD_MUST_BE_VOLATILE:
            return "ChildMustBeVolatile";
        case SMB2_STATUS_DEVICE_CONFIGURATION_ERROR:
            return "DeviceConfigurationError";
        case SMB2_STATUS_DRIVER_INTERNAL_ERROR:
            return "DriverInternalError";
        case SMB2_STATUS_INVALID_DEVICE_STATE:
            return "InvalidDeviceState";
        case SMB2_STATUS_IO_DEVICE_ERROR:
            return "IoDeviceError";
        case SMB2_STATUS_DEVICE_PROTOCOL_ERROR:
            return "DeviceProtocolError";
        case SMB2_STATUS_BACKUP_CONTROLLER:
            return "BackupController";
        case SMB2_STATUS_LOG_FILE_FULL:
            return "LogFileFull";
        case SMB2_STATUS_TOO_LATE:
            return "TooLate";
        case SMB2_STATUS_NO_TRUST_LSA_SECRET:
            return "NoTrustLsaSecret";
        case SMB2_STATUS_NO_TRUST_SAM_ACCOUNT:
            return "NoTrustSamAccount";
        case SMB2_STATUS_TRUSTED_DOMAIN_FAILURE:
            return "TrustedDomainFailure";
        case SMB2_STATUS_TRUSTED_RELATIONSHIP_FAILURE:
            return "TrustedRelationshipFailure";
        case SMB2_STATUS_EVENTLOG_FILE_CORRUPT:
            return "EventlogFileCorrupt";
        case SMB2_STATUS_EVENTLOG_CANT_START:
            return "EventlogCantStart";
        case SMB2_STATUS_TRUST_FAILURE:
            return "TrustFailure";
        case SMB2_STATUS_MUTANT_LIMIT_EXCEEDED:
            return "MutantLimitExceeded";
        case SMB2_STATUS_NETLOGON_NOT_STARTED:
            return "NetlogonNotStarted";
        case SMB2_STATUS_ACCOUNT_EXPIRED:
            return "AccountExpired";
        case SMB2_STATUS_POSSIBLE_DEADLOCK:
            return "PossibleDeadlock";
        case SMB2_STATUS_NETWORK_CREDENTIAL_CONFLICT:
            return "NetworkCredentialConflict";
        case SMB2_STATUS_REMOTE_SESSION_LIMIT:
            return "RemoteSessionLimit";
        case SMB2_STATUS_EVENTLOG_FILE_CHANGED:
            return "EventlogFileChanged";
        case SMB2_STATUS_NOLOGON_INTERDOMAIN_TRUST_ACCOUNT:
            return "NologonInterdomainTrustAccount";
        case SMB2_STATUS_NOLOGON_WORKSTATION_TRUST_ACCOUNT:
            return "NologonWorkstationTrustAccount";
        case SMB2_STATUS_NOLOGON_SERVER_TRUST_ACCOUNT:
            return "NologonServerTrustAccount";
        case SMB2_STATUS_DOMAIN_TRUST_INCONSISTENT:
            return "DomainTrustInconsistent";
        case SMB2_STATUS_FS_DRIVER_REQUIRED:
            return "FsDriverRequired";
        case SMB2_STATUS_NO_USER_SESSION_KEY:
            return "NoUserSessionKey";
        case SMB2_STATUS_USER_SESSION_DELETED:
            return "UserSessionDeleted";
        case SMB2_STATUS_RESOURCE_LANG_NOT_FOUND:
            return "ResourceLangNotFound";
        case SMB2_STATUS_INSUFF_SERVER_RESOURCES:
            return "InsuffServerResources";
        case SMB2_STATUS_INVALID_BUFFER_SIZE:
            return "InvalidBufferSize";
        case SMB2_STATUS_INVALID_ADDRESS_COMPONENT:
            return "InvalidAddressComponent";
        case SMB2_STATUS_INVALID_ADDRESS_WILDCARD:
            return "InvalidAddressWildcard";
        case SMB2_STATUS_TOO_MANY_ADDRESSES:
            return "TooManyAddresses";
        case SMB2_STATUS_ADDRESS_ALREADY_EXISTS:
            return "AddressAlreadyExists";
        case SMB2_STATUS_ADDRESS_CLOSED:
            return "AddressClosed";
        case SMB2_STATUS_CONNECTION_DISCONNECTED:
            return "ConnectionDisconnected";
        case SMB2_STATUS_CONNECTION_RESET:
            return "ConnectionReset";
        case SMB2_STATUS_TOO_MANY_NODES:
            return "TooManyNodes";
        case SMB2_STATUS_TRANSACTION_ABORTED:
            return "TransactionAborted";
        case SMB2_STATUS_TRANSACTION_TIMED_OUT:
            return "TransactionTimedOut";
        case SMB2_STATUS_TRANSACTION_NO_RELEASE:
            return "TransactionNoRelease";
        case SMB2_STATUS_TRANSACTION_NO_MATCH:
            return "TransactionNoMatch";
        case SMB2_STATUS_TRANSACTION_RESPONDED:
            return "TransactionResponded";
        case SMB2_STATUS_TRANSACTION_INVALID_ID:
            return "TransactionInvalidId";
        case SMB2_STATUS_TRANSACTION_INVALID_TYPE:
            return "TransactionInvalidType";
        case SMB2_STATUS_NOT_SERVER_SESSION:
            return "NotServerSession";
        case SMB2_STATUS_NOT_CLIENT_SESSION:
            return "NotClientSession";
        case SMB2_STATUS_CANNOT_LOAD_REGISTRY_FILE:
            return "CannotLoadRegistryFile";
        case SMB2_STATUS_DEBUG_ATTACH_FAILED:
            return "DebugAttachFailed";
        case SMB2_STATUS_SYSTEM_PROCESS_TERMINATED:
            return "SystemProcessTerminated";
        case SMB2_STATUS_DATA_NOT_ACCEPTED:
            return "DataNotAccepted";
        case SMB2_STATUS_NO_BROWSER_SERVERS_FOUND:
            return "NoBrowserServersFound";
        case SMB2_STATUS_VDM_HARD_ERROR:
            return "VdmHardError";
        case SMB2_STATUS_DRIVER_CANCEL_TIMEOUT:
            return "DriverCancelTimeout";
        case SMB2_STATUS_REPLY_MESSAGE_MISMATCH:
            return "ReplyMessageMismatch";
        case SMB2_STATUS_MAPPED_ALIGNMENT:
            return "MappedAlignment";
        case SMB2_STATUS_IMAGE_CHECKSUM_MISMATCH:
            return "ImageChecksumMismatch";
        case SMB2_STATUS_LOST_WRITEBEHIND_DATA:
            return "LostWritebehindData";
        case SMB2_STATUS_CLIENT_SERVER_PARAMETERS_INVALID:
            return "ClientServerParametersInvalid";
        case SMB2_STATUS_PASSWORD_MUST_CHANGE:
            return "PasswordMustChange";
        case SMB2_STATUS_NOT_FOUND:
            return "NotFound";
        case SMB2_STATUS_NOT_TINY_STREAM:
            return "NotTinyStream";
        case SMB2_STATUS_RECOVERY_FAILURE:
            return "RecoveryFailure";
        case SMB2_STATUS_STACK_OVERFLOW_READ:
            return "StackOverflowRead";
        case SMB2_STATUS_FAIL_CHECK:
            return "FailCheck";
        case SMB2_STATUS_DUPLICATE_OBJECTID:
            return "DuplicateObjectid";
        case SMB2_STATUS_OBJECTID_EXISTS:
            return "ObjectidExists";
        case SMB2_STATUS_CONVERT_TO_LARGE:
            return "ConvertToLarge";
        case SMB2_STATUS_RETRY:
            return "Retry";
        case SMB2_STATUS_FOUND_OUT_OF_SCOPE:
            return "FoundOutOfScope";
        case SMB2_STATUS_ALLOCATE_BUCKET:
            return "AllocateBucket";
        case SMB2_STATUS_PROPSET_NOT_FOUND:
            return "PropsetNotFound";
        case SMB2_STATUS_MARSHALL_OVERFLOW:
            return "MarshallOverflow";
        case SMB2_STATUS_INVALID_VARIANT:
            return "InvalidVariant";
        case SMB2_STATUS_DOMAIN_CONTROLLER_NOT_FOUND:
            return "DomainControllerNotFound";
        case SMB2_STATUS_ACCOUNT_LOCKED_OUT:
            return "AccountLockedOut";
        case SMB2_STATUS_HANDLE_NOT_CLOSABLE:
            return "HandleNotClosable";
        case SMB2_STATUS_CONNECTION_REFUSED:
            return "ConnectionRefused";
        case SMB2_STATUS_GRACEFUL_DISCONNECT:
            return "GracefulDisconnect";
        case SMB2_STATUS_ADDRESS_ALREADY_ASSOCIATED:
            return "AddressAlreadyAssociated";
        case SMB2_STATUS_ADDRESS_NOT_ASSOCIATED:
            return "AddressNotAssociated";
        case SMB2_STATUS_CONNECTION_INVALID:
            return "ConnectionInvalid";
        case SMB2_STATUS_CONNECTION_ACTIVE:
            return "ConnectionActive";
        case SMB2_STATUS_NETWORK_UNREACHABLE:
            return "NetworkUnreachable";
        case SMB2_STATUS_HOST_UNREACHABLE:
            return "HostUnreachable";
        case SMB2_STATUS_PROTOCOL_UNREACHABLE:
            return "ProtocolUnreachable";
        case SMB2_STATUS_PORT_UNREACHABLE:
            return "PortUnreachable";
        case SMB2_STATUS_REQUEST_ABORTED:
            return "RequestAborted";
        case SMB2_STATUS_CONNECTION_ABORTED:
            return "ConnectionAborted";
        case SMB2_STATUS_BAD_COMPRESSION_BUFFER:
            return "BadCompressionBuffer";
        case SMB2_STATUS_USER_MAPPED_FILE:
            return "UserMappedFile";
        case SMB2_STATUS_AUDIT_FAILED:
            return "AuditFailed";
        case SMB2_STATUS_TIMER_RESOLUTION_NOT_SET:
            return "TimerResolutionNotSet";
        case SMB2_STATUS_CONNECTION_COUNT_LIMIT:
            return "ConnectionCountLimit";
        case SMB2_STATUS_LOGIN_TIME_RESTRICTION:
            return "LoginTimeRestriction";
        case SMB2_STATUS_LOGIN_WKSTA_RESTRICTION:
            return "LoginWkstaRestriction";
        case SMB2_STATUS_IMAGE_MP_UP_MISMATCH:
            return "ImageMpUpMismatch";
        case SMB2_STATUS_INSUFFICIENT_LOGON_INFO:
            return "InsufficientLogonInfo";
        case SMB2_STATUS_BAD_DLL_ENTRYPOINT:
            return "BadDllEntrypoint";
        case SMB2_STATUS_BAD_SERVICE_ENTRYPOINT:
            return "BadServiceEntrypoint";
        case SMB2_STATUS_LPC_REPLY_LOST:
            return "LpcReplyLost";
        case SMB2_STATUS_IP_ADDRESS_CONFLICT1:
            return "IpAddressConflict1";
        case SMB2_STATUS_IP_ADDRESS_CONFLICT2:
            return "IpAddressConflict2";
        case SMB2_STATUS_REGISTRY_QUOTA_LIMIT:
            return "RegistryQuotaLimit";
        case SMB2_STATUS_PATH_NOT_COVERED:
            return "PathNotCovered";
        case SMB2_STATUS_NO_CALLBACK_ACTIVE:
            return "NoCallbackActive";
        case SMB2_STATUS_LICENSE_QUOTA_EXCEEDED:
            return "LicenseQuotaExceeded";
        case SMB2_STATUS_PWD_TOO_SHORT:
            return "PwdTooShort";
        case SMB2_STATUS_PWD_TOO_RECENT:
            return "PwdTooRecent";
        case SMB2_STATUS_PWD_HISTORY_CONFLICT:
            return "PwdHistoryConflict";
        case SMB2_STATUS_PLUGPLAY_NO_DEVICE:
            return "PlugplayNoDevice";
        case SMB2_STATUS_UNSUPPORTED_COMPRESSION:
            return "UnsupportedCompression";
        case SMB2_STATUS_INVALID_HW_PROFILE:
            return "InvalidHwProfile";
        case SMB2_STATUS_INVALID_PLUGPLAY_DEVICE_PATH:
            return "InvalidPlugplayDevicePath";
        case SMB2_STATUS_DRIVER_ORDINAL_NOT_FOUND:
            return "DriverOrdinalNotFound";
        case SMB2_STATUS_DRIVER_ENTRYPOINT_NOT_FOUND:
            return "DriverEntrypointNotFound";
        case SMB2_STATUS_RESOURCE_NOT_OWNED:
            return "ResourceNotOwned";
        case SMB2_STATUS_TOO_MANY_LINKS:
            return "TooManyLinks";
        case SMB2_STATUS_QUOTA_LIST_INCONSISTENT:
            return "QuotaListInconsistent";
        case SMB2_STATUS_FILE_IS_OFFLINE:
            return "FileIsOffline";
        case SMB2_STATUS_VOLUME_DISMOUNTED:
            return "VolumeDismounted";
        case SMB2_STATUS_NOT_A_REPARSE_POINT:
            return "NotAReparsePoint";
        case SMB2_STATUS_SERVER_UNAVAILABLE:
            return "ServerUnavailable";
        case SMB2_STATUS_BUFFER_OVERFLOW:
            return "BufferOverflow";
        case SMB2_STATUS_STOPPED_ON_SYMLINK:
            return "StoppedOnSymlink";
        default:
            return "Unknown";
    } /* switch */
} /* smb_status_name */

static const char *
smb_create_disposition_name(uint32_t disposition)
{
    switch (disposition) {
        case SMB2_FILE_CREATE:
            return "Create";
        case SMB2_FILE_OPEN:
            return "Open";
        case SMB2_FILE_OPEN_IF:
            return "OpenIf";
        case SMB2_FILE_OVERWRITE:
            return "Overwrite";
        case SMB2_FILE_OVERWRITE_IF:
            return "OverwriteIf";
        default:
            return "Unknown";
    } /* switch */
} /* smb_create_disposition_name */

static const char *
smb_dialect_name(uint16_t dialect)
{
    switch (dialect) {
        case 0x0210:
            return "SMB2.1";
        case 0x02ff:
            return "SMB2.??";
        case 0x0300:
            return "SMB3.0";
        case 0x0302:
            return "SMB3.02";
        case 0x0311:
            return "SMB3.11";
        default:
            return "Unknown";
    } /* switch */

} /* smb_dialect_name */

static const char *
smb_ioctl_ctl_code_name(uint32_t ctl_code)
{
    switch (ctl_code) {
        case SMB2_FSCTL_VALIDATE_NEGOTIATE_INFO:
            return "VALIDATE_NEGOTIATE_INFO";
        case SMB2_FSCTL_TRANSCEIVE_PIPE:
            return "TRANSCEIVE_PIPE";
        case SMB2_FSCTL_DFS_GET_REFERRALS:
            return "DFS_GET_REFERRALS";
        default:
            return "Unknown";
    } /* switch */
} /* smb_ioctl_ctl_code_name */

void
_smb_dump_request(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[160];
    char *hdrp = hdr_args;

    *hdrp = '\0';

    if (request->smb2_hdr.session_id) {
        hdrp += sprintf(hdrp, " session %lx", request->smb2_hdr.session_id);
    }

    if (request->smb2_hdr.sync.tree_id) {
        if (request->tree) {
            sprintf(hdrp, " tree %s(%x)",
                    request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE ? "IPC$" : request->tree->share->name,
                    request->smb2_hdr.sync.tree_id);
        } else {
            sprintf(hdrp, " tree_id %x", request->smb2_hdr.sync.tree_id);
        }
    }

    switch (request->smb2_hdr.command) {
        case SMB2_TREE_CONNECT:
            sprintf(argstr, " path %.*s", request->tree_connect.path_length, request->tree_connect.path);
            break;
        case SMB2_CREATE:
            sprintf(argstr, " parent_path %.*s name %.*s create_disposition %s create_options %x desired_access %x",
                    request->create.parent_path_len, request->create.parent_path,
                    request->create.name_len, request->create.name,
                    smb_create_disposition_name(request->create.create_disposition),
                    request->create.create_options,
                    request->create.desired_access);
            break;
        case SMB2_CLOSE:
            if (request->close.file_id.pid != UINT64_MAX) {
                sprintf(argstr, " file_id %lx.%lx", request->close.file_id.pid, request->close.file_id.vid);
            }
            break;
        case SMB2_WRITE:
            sprintf(argstr, " file_id %lx.%lx offset %lu length %u write_through %d",
                    request->write.file_id.pid, request->write.file_id.vid,
                    request->write.offset, request->write.length,
                    !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH));
            break;
        case SMB2_READ:
            sprintf(argstr, " file_id %lx.%lx offset %lu length %u",
                    request->read.file_id.pid, request->read.file_id.vid,
                    request->read.offset, request->read.length);
            break;
        case SMB2_IOCTL:
            sprintf(argstr, " file_id %lx.%lx ctl_code %s count %u",
                    request->ioctl.file_id.pid, request->ioctl.file_id.vid,
                    smb_ioctl_ctl_code_name(request->ioctl.ctl_code),
                    request->ioctl.input_count);
            break;
        case SMB2_SET_INFO:
            sprintf(argstr, " file_id %lx.%lx info_type %u info_class %u addl_info %u",
                    request->set_info.file_id.pid, request->set_info.file_id.vid,
                    request->set_info.info_type, request->set_info.info_class,
                    request->set_info.addl_info);
            break;
        case SMB2_QUERY_INFO:
            sprintf(argstr, " file_id %lx.%lx info_type %u info_class %u addl_info %u flags %u",
                    request->query_info.file_id.pid, request->query_info.file_id.vid,
                    request->query_info.info_type, request->query_info.info_class,
                    request->query_info.addl_info, request->query_info.flags);
            break;
        case SMB2_QUERY_DIRECTORY:
            sprintf(argstr, " file_id %lx.%lx flags %x info_class %u file_index %u pattern %.*s",
                    request->query_directory.file_id.pid, request->query_directory.file_id.vid,
                    request->query_directory.flags, request->query_directory.info_class,
                    request->query_directory.file_index, request->query_directory.pattern_length,
                    request->query_directory.pattern);
            break;
        default:
            argstr[0] = '\0';
    } /* switch */

    chimera_smb_debug("SMB  Request %p: %d/%d Msgid %08x %s%s%s", request, i, n,
                      request->smb2_hdr.message_id,
                      smb_command_name(request->smb2_hdr.command),
                      hdr_args,
                      argstr);
} /* _smb_dump_request */

void
_smb_dump_reply(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[80];
    char *hdrp = hdr_args;

    argstr[0] = '\0';

    switch (request->smb2_hdr.command) {
        case SMB2_NEGOTIATE:
            sprintf(argstr, " dialect %s", smb_dialect_name(request->negotiate.r_dialect));
            break;
        case SMB2_CREATE:
            if (request->status == SMB2_STATUS_SUCCESS) {
                sprintf(argstr, " file_id %lx.%lx",
                        request->create.r_open_file->file_id.pid,
                        request->create.r_open_file->file_id.vid);
            }
            break;
        default:
            break;
    } /* switch */

    *hdrp = '\0';

    if (request->session_handle && request->session_handle->session) {
        hdrp += sprintf(hdrp, " sessiond %lx", request->session_handle->session->session_id);
    }

    if (request->tree) {
        sprintf(hdrp, " tree_id %x", request->tree->tree_id);
    }

    chimera_smb_debug("SMB  Reply   %p: %d/%d MsgId %08x %s %s%s%s",
                      request, i, n, request->smb2_hdr.message_id,
                      smb_command_name(request->smb2_hdr.command),
                      smb_status_name(request->status), hdr_args, argstr);
} /* _smb_dump_request_reply */

void
_smb_dump_compound_request(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_request(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_request */

void
_smb_dump_compound_reply(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_reply(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_reply */