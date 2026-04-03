package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.dialect.IcebergSqlDialect;
import org.junit.jupiter.api.Test;
import java.util.Map;

class IcebergDiagTest {
    private final GremlinSqlTranslator translator = new GremlinSqlTranslator(new IcebergSqlDialect());

    private MappingConfig m() {
        return new MappingConfig(
            Map.of(
                "Account", new VertexMapping("iceberg:aml.accounts", "id", Map.of(
                        "accountId","account_id","bankId","bank_id","riskScore","risk_score"
                )),
                "Bank", new VertexMapping("iceberg:aml.banks", "id", Map.of(
                        "bankId","bank_id","bankName","bank_name","countryCode","country_code"
                )),
                "Country", new VertexMapping("iceberg:aml.countries", "id", Map.of(
                        "countryCode","country_code","countryName","country_name"
                )),
                "Alert", new VertexMapping("iceberg:aml.alerts", "id", Map.of(
                        "alertId","alert_id","severity","severity","status","status","alertType","alert_type","raisedAt","raised_at"
                ))
            ),
            Map.of(
                "FLAGGED_BY", new EdgeMapping("iceberg:aml.account_alert", "id", "out_id", "in_id", Map.of()),
                "BELONGS_TO", new EdgeMapping("iceberg:aml.account_bank",  "id", "out_id", "in_id", Map.of()),
                "LOCATED_IN", new EdgeMapping("iceberg:aml.bank_country",  "id", "out_id", "in_id", Map.of())
            )
        );
    }

    @Test
    void diagC7() {
        var r = translator.translate(
                "g.V().hasLabel('Account').where(outE('FLAGGED_BY'))" +
                ".project('accountId','bankId','alertCount','highSeverity')" +
                ".by('accountId').by('bankId')" +
                ".by(outE('FLAGGED_BY').count())" +
                ".by(out('FLAGGED_BY').has('severity','HIGH').count()).limit(20)",
                m());
        System.out.println("C7 SQL: " + r.sql());
    }

    @Test
    void diagArrayJoin() {
        var r = translator.translate(
                "g.V().hasLabel('Bank').limit(3)" +
                ".project('bankId','countryName')" +
                ".by('bankId').by(out('LOCATED_IN').values('countryName').fold())",
                m());
        System.out.println("ARRAY_JOIN SQL: " + r.sql());
    }
}

