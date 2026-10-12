from __future__ import annotations


def index_schema(config):
    properties = {}
    keyword = "ChunkId DocId ParentDocId RootDocId SourceIndex SourceEsId SourceKey FileKey ContainerFileKey MemberId FileExtension DetectedMimeType ReportType Branch ParentSourceIndex ParentSourceEsId ParentResolutionStatus MetadataOrigin MetadataInheritedFields Modality SourceMediaType RepresentationKind ContentSource ExtractionMethod ExtractorVersion TextQualityStatus TextQualityFlags Language FileAvailability ProcessingStatus WarningCodes OffsetBasis NormalizedTextRef SectionPath SheetName CellRange CoordinateSystem MemberPath MemberMimeType EmbeddingModel ModelRevision EmbeddingTask ProcessorVersion TokenizerRevision SourceTextHash ContentHash SourceVersion ChunkingVersion PipelineVersion ConfigHash GenerationId IngestionRunId TenantId SecurityClassification AllowedPrincipals ACLVersion ACLSource".split()
    integer = "ChunkNumber TokenCount PageStart PageEnd SlideNumber RowStart RowEnd FrameNumber Channel ImageWidth ImageHeight MemberOrdinal ArchiveDepth EmbeddingDimensions".split()
    long = "CharStart CharEnd FileSizeBytes SourceSeqNo SourcePrimaryTerm".split()
    boolean = "FileExists UsedTextFallback PreviewAvailable CoverageComplete EmbeddingNormalized".split()
    doubles = "StartSeconds EndSeconds MediaDurationSeconds".split()
    for names, kind in [(keyword, "keyword"), (integer, "integer"), (long, "long"), (boolean, "boolean"), (doubles, "double")]:
        for name in names:
            properties[name] = {"type": kind}
    properties.update({
        "OriginalName": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}}},
        "SystemPath": {"type": "keyword", "index": False, "doc_values": False},
        "DocumentDateRaw": {"type": "keyword", "index": False, "doc_values": False},
        "ChunkText": {"type": "text"},
        "SourceMetadata": {"type": "object", "enabled": False},
        "ArchiveChain": {"type": "object", "enabled": False},
        "BoundingBox": {"type": "object", "enabled": False},
        "DocumentDate": {"type": "date", "format": "strict_date_optional_time"},
        "FileModifiedAt": {"type": "date"}, "IngestedAt": {"type": "date"},
        "Embedding": {"type": "dense_vector", "dims": config.embed["dimensions"], "index": True,
            "similarity": "cosine", "index_options": {"type": config.es["vector_index_type"]}},
    })
    return {"settings": {"number_of_shards": config.es["shards"], "number_of_replicas": config.es["replicas"], "refresh_interval": "30s"},
        "mappings": {"dynamic": "strict", "_meta": {"config_hash": config.signature, "pipeline": "reports_ingest", "build_status": "unpublished"}, "properties": properties}}

