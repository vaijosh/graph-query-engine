package com.graphqueryengine.query.translate.sql.model;

/** Intermediate result from parsing a {@code repeat(...).times(n)} pair. */
public record RepeatHopResult(HopStep hop, boolean simplePathDetected) {}

