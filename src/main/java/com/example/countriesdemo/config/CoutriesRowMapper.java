package com.example.countriesdemo.config;

import com.example.countriesdemo.data.Countries;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class CoutriesRowMapper implements RowMapper<Countries> {

    @Override
    public Countries mapRow(ResultSet rs, int rowNum) throws SQLException {

        Countries c =  new Countries();
        c.setId(rs.getInt("Id"));
        c.setCountries(rs.getString("name"));

        return c;
    }
}
