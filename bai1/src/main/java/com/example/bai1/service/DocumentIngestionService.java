package com.example.bai1.service;

import lombok.RequiredArgsConstructor;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.pgvector.PgVectorStore;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static reactor.netty.http.HttpConnectionLiveness.log;

@Service
@RequiredArgsConstructor
public class DocumentIngestionService {
    private final PgVectorStore vectorStore;

    public int ingestDocumentWithTika(Resource resource, String category, Map<String, Object> customMetadata) {
        String filename = (resource != null && resource.getFilename() != null)
                ? resource.getFilename()
                : "unknown_file";

        log.info("==> [ETL Tika Pipeline] Bắt đầu xử lý file: {} | Category: {}", filename, category);

        // 1. Kiểm tra tính tồn tại của file
        if (resource == null || !resource.exists()) {
            log.error("==> [ETL Error] File tài liệu không tồn tại: {}", filename);
            throw new IllegalArgumentException("Resource không tồn tại: " + filename);
        }

        try {
            // 2. EXTRACT: Đọc file thông qua Apache Tika Document Reader
            TikaDocumentReader tikaReader = new TikaDocumentReader(resource);
            List<Document> rawDocuments = tikaReader.get();

            if (rawDocuments.isEmpty()) {
                log.warn("==> [ETL Warning] Apache Tika không thể trích xuất nội dung từ file '{}'", filename);
                return 0;
            }

            log.info("==> [ETL Extract] Tika trích xuất thành công {} Document(s) thô từ '{}'",
                    rawDocuments.size(), filename);

            // 3. TRANSFORM: Chia nhỏ bằng TokenTextSplitter theo thông số đề bài
            // chunkSize = 600, minChunkSizeChars = 120, maxTokens = 10000
            TokenTextSplitter tokenSplitter = TokenTextSplitter.builder()
                    .withChunkSize(800)
                    .withMinChunkSizeChars(100)
                    .withKeepSeparator(true)
                    .withMaxNumChunks(10000)
                    .build();

            List<Document> chunkedDocuments = tokenSplitter.apply(rawDocuments);

            if (chunkedDocuments.isEmpty()) {
                log.warn("==> [ETL Warning] Không có chunk nào hợp lệ sau khi cắt từ file '{}'", filename);
                return 0;
            }

            // 4. BỔ SUNG DYNAMIC METADATA (category, source_file, timestamps,...)
            String timestamp = Instant.now().toString();
            for (int i = 0; i < chunkedDocuments.size(); i++) {
                Document doc = chunkedDocuments.get(i);
                Map<String, Object> metadata = doc.getMetadata();

                // Lưu ý: Tika đã tự động trích xuất sẵn một số metadata hệ thống (như mime-type, author nếu có trong PDF/DOCX)
                // Ta bổ sung thêm các dynamic metadata của nghiệp vụ:
                metadata.put("source_file", filename);
                metadata.put("category", (category != null && !category.isBlank()) ? category : "general");
                metadata.put("chunk_index", i);
                metadata.put("total_chunks", chunkedDocuments.size());
                metadata.put("ingested_at", timestamp);
                metadata.put("parser", "apache_tika");

                if (customMetadata != null && !customMetadata.isEmpty()) {
                    metadata.putAll(customMetadata);
                }
            }

            log.info("==> [ETL Transform] Chia thành {} chunks. Bắt đầu embedding và lưu vào Supabase...",
                    chunkedDocuments.size());

            // 5. LOAD: Nạp vào pgvector VectorStore (Gọi Ollama nomic-embed-text)
            vectorStore.accept(chunkedDocuments);

            log.info("==> [ETL Load] Nạp THÀNH CÔNG {} chunks từ file '{}' vào Supabase Vector Store!",
                    chunkedDocuments.size(), filename);

            return chunkedDocuments.size();

        } catch (Exception ex) {
            log.error("==> [ETL Error] Lỗi khi xử lý file qua Apache Tika '{}': {}", filename, ex.getMessage(), ex);
            throw new RuntimeException("Tiến trình ETL qua Tika thất bại: " + filename, ex);
        }
    }
}
