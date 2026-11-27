package metrics

// AttachLimit identifies the dynamic config setting that rejected an attachment.
type AttachLimit string

const attachLimitTagName = "limit"

const (
	AttachLimitMaxCallbacksPerExecution  AttachLimit = "max_callbacks_per_execution"  // callback.maxPerExecution
	AttachLimitMaxCallbacksPerWorkflow   AttachLimit = "max_callbacks_per_workflow"   // system.maxCallbacksPerWorkflow
	AttachLimitTotalCallbacksMaxSize     AttachLimit = "total_callbacks_max_size"     // callback.totalMaxSizePerExecution
	AttachLimitCallbackURLMaxLength      AttachLimit = "callback_url_max_length"      // frontend.callbackURLMaxLength
	AttachLimitCallbackHeaderMaxSize     AttachLimit = "callback_header_max_size"     // frontend.callbackHeaderMaxLength
	AttachLimitCallbackSourceContextSize AttachLimit = "callback_source_context_size" // callback.nexusHandler.sourceContext.maxSize
	AttachLimitMaxIDLength               AttachLimit = "max_id_length"                // limit.maxIDLength
	AttachLimitNexusServiceNameLength    AttachLimit = "nexus_service_name_length"    // nexusoperation.limit.service.name.length
	AttachLimitNexusOperationNameLength  AttachLimit = "nexus_operation_name_length"  // nexusoperation.limit.operation.name.length
	AttachLimitMaxLinksPerRequest        AttachLimit = "max_links_per_request"        // frontend.maxLinksPerRequest
	AttachLimitMaxLinksPerComponent      AttachLimit = "max_links_per_component"      // chasm.maxLinksPerComponent
	AttachLimitLinkMaxSize               AttachLimit = "link_max_size"                // frontend.linkMaxSize
	AttachLimitTotalLinksMaxSize         AttachLimit = "total_links_max_size"         // chasm.totalLinksMaxSizePerComponent
)

// RecordAttachLimitExceeded records a rejection against the setting that enforced the limit.
func RecordAttachLimitExceeded(handler Handler, namespace string, limit AttachLimit) {
	AttachLimitExceeded.With(handler).Record(1, NamespaceTag(namespace), Tag{Key: attachLimitTagName, Value: string(limit)})
}
