package com.rethinkdb.gen.ast;

import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.proto.TermType;
import com.rethinkdb.model.Arguments;

public class CljFunc extends ReqlExpr {
    public CljFunc(Arguments args) {
        super(TermType.FUNC, args, null);
    }
}
