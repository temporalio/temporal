if (ctx._source.Attr != null) {
    if (ctx._source.Attr.CustomKeywordField != null) {ctx._source.CustomKeywordField = ctx._source.Attr.CustomKeywordField;}
    if (ctx._source.Attr.CustomStringField != null) {ctx._source.CustomStringField = ctx._source.Attr.CustomStringField;}
    if (ctx._source.Attr.CustomIntField != null) {ctx._source.CustomIntField = ctx._source.Attr.CustomIntField;}
    if (ctx._source.Attr.CustomDatetimeField != null) {ctx._source.CustomDatetimeField = ctx._source.Attr.CustomDatetimeField;}
    if (ctx._source.Attr.CustomDoubleField != null) {ctx._source.CustomDoubleField = ctx._source.Attr.CustomDoubleField;}
    if (ctx._source.Attr.CustomBoolField != null) {ctx._source.CustomBoolField = ctx._source.Attr.CustomBoolField;}

    if (ctx._source.Attr.TemporalChangeVersion != null) {ctx._source.TemporalChangeVersion = ctx._source.Attr.TemporalChangeVersion;}
    if (ctx._source.Attr.CustomNamespace != null) {ctx._source.CustomNamespace = ctx._source.Attr.CustomNamespace;}
    if (ctx._source.Attr.Operator != null) {ctx._source.Operator = ctx._source.Attr.Operator;}
    if (ctx._source.Attr.BinaryChecksums != null) {ctx._source.BinaryChecksums = ctx._source.Attr.BinaryChecksums;}
    ctx._source.remove('Attr');
}
