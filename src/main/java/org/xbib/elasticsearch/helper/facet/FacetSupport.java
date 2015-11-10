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
package org.xbib.elasticsearch.helper.facet;

import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.xbib.facet.Facet;
import org.xbib.facet.FacetListener;
import org.xbib.facet.FacetTerm;

import java.util.Iterator;

/**
 * Facet support
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class FacetSupport {

    private final FacetListener listener;

    public FacetSupport(FacetListener listener) {
        this.listener = listener;
    }

    public void parse(Facets facets) {
        if (facets == null) {
            return;
        }
        if (facets.facets() == null) {
            return;
        }
        Iterator<org.elasticsearch.search.facet.Facet> it = facets.facets().iterator();
        while (it.hasNext()) {
            org.elasticsearch.search.facet.Facet f = it.next();
            if (f instanceof TermsFacet) {
                TermsFacet tf = (TermsFacet) f;
                // String displayLabel, String description, String index, String relation
                Facet facet = new Facet("", "", f.getName(), "=");
                for (TermsFacet.Entry e : tf.getEntries()) {
                    FacetTerm term = new FacetTerm(e.getTerm().string(), e.getCount(), null, null);
                    facet.add(term);
                }
                listener.receive(facet);
            }
        }
    }

}
