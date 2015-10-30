package com.hcse.app.data.mysql;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import javax.annotation.Resource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.core.support.AbstractLobStreamingResultSetExtractor;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.stereotype.Repository;


@Repository
public class DocumentDao {
    @Resource(name = "hcseJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    final LobHandler lobHandler = new DefaultLobHandler();

    class BlobExtractorOne extends AbstractLobStreamingResultSetExtractor {
        private DbDocument doc = new DbDocument();

        @Override
        protected void streamData(ResultSet rs) throws SQLException, IOException, DataAccessException {
        	doc.setKey(rs.getLong(1));
        	doc.setContent(lobHandler.getBlobAsBytes(rs, 2));
        }

        public DbDocument getDoc() {
            return doc;
        }
    }

    class BlobExtractorMultiple extends AbstractLobStreamingResultSetExtractor {
        private HashMap<Long, DbDocument> docs = new HashMap<Long, DbDocument>();

        @Override
        protected void streamData(ResultSet rs) throws SQLException, IOException, DataAccessException {
        	DbDocument doc = new DbDocument();
        	
        	doc.setKey(rs.getLong(1));
        	doc.setContent(lobHandler.getBlobAsBytes(rs, 2));
        	
        	docs.put(doc.getKey(), doc);
        }

        public HashMap<Long, DbDocument> getDocs() {
            return docs;
        }
    }

    @SuppressWarnings("unchecked")
	public DbDocument getContentByKey(final long key) {
        String sql = "select `id`,`content` from hcse_doc where `key`=?";

        BlobExtractorOne extractor = new BlobExtractorOne();

        jdbcTemplate.query(sql, new Object[] { key }, extractor);

        return extractor.getDoc();
    }
    
    @SuppressWarnings("unchecked")
	public HashMap<Long, DbDocument> getContentByKeys(final Long[] ids) {
        String _sql = "select `id`,`content` from hcse_doc where `key` in (";
        
        //build sql
        StringBuilder sb = new StringBuilder();
        
        sb.append(_sql);
        
        for(int i=0; i<ids.length; i++){
        	sb.append("?,");
        }

        sb.setCharAt(sb.length()-1, ')');
        
        //exec sql
        BlobExtractorMultiple extractor = new BlobExtractorMultiple();

        jdbcTemplate.query(sb.toString(), ids, extractor);

        return extractor.getDocs();
    }
    
    public void insert(final DbDocument doc) {

        String sql = "insert into hcse_document(`key`,`content`) values (?, ?)";
        
        jdbcTemplate.execute(sql, new AbstractLobCreatingPreparedStatementCallback(lobHandler) {
            protected void setValues(PreparedStatement pstmt, LobCreator lobCreator) throws SQLException,
                    DataAccessException {

                pstmt.setLong(1, doc.getKey());

                lobCreator.setBlobAsBytes(pstmt, 2, doc.getContent());
            }
        });
    }

    public void deleteById(final long id) {
        String sql = "delete from hcse_document where `id`=?";
        
        jdbcTemplate.update(sql, id);
    }
    
    public void deleteByKey(final long key) {
        String sql = "delete from hcse_document where `key`=?";
        
        jdbcTemplate.update(sql, key);
    }
}
