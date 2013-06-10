/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.facet;

import java.util.ArrayList;
import java.util.List;

public class Facet {

    private String displayLabel;

    private String description;

    private String index;

    private String relation;

    private List<FacetTerm> terms = new ArrayList();

    public Facet(String displayLabel, String description, String index, String relation) {
        this.displayLabel = displayLabel;
        this.description = description;
        this.index = index;
        this.relation = relation;
        this.terms = new ArrayList();
    }

    public void add(FacetTerm term) {
        terms.add(term);
    }

    public String getDisplayLabel() {
        return displayLabel;
    }

    public String getDescription() {
        return description;
    }

    public String getIndex() {
        return index;
    }

    public String getRelation() {
        return relation;
    }

    public List<FacetTerm> getTerms() {
        return terms;
    }

}
