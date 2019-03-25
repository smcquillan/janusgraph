// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.optimize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class JanusGraphUnionStep<S, E> extends BranchStep<S, E, TraversalOptionParent.Pick> {

    private boolean initialized = false;

    public JanusGraphUnionStep(UnionStep<S, E> originalStep) {
        super(originalStep.getTraversal());

        this.setBranchTraversal(new ConstantTraversal<>(Pick.any));
        for (final Traversal.Admin<?, ?> globalChild : originalStep.getGlobalChildren()) {
            globalChild.removeStep(globalChild.getEndStep());
            this.addGlobalChildOption(Pick.any, (Traversal.Admin) globalChild);
        }
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversalOptions.getOrDefault(Pick.any, Collections.emptyList()));
    }

    /**
     * This is a union so all options will be taken but the BranchStep class will drip feed in the
     * vertices one at a time. To exploit the multiQuery offered by JanusGraphVertexStep we need to
     * initialise it will all the vertices allowing it to later retrieve them one at a time without
     * a new trip to the storage back end.
     */
    private void initialize() {
        assert !initialized;
        initialized = true;
        final List<Traverser.Admin<S>> vertices = new ArrayList<>();

        for (final List<Traversal.Admin<S, E>> options : this.traversalOptions.values()) {
            for (final Traversal.Admin<S, E> option : options) {
                Step<S, ?> step = option.getStartStep();
                if (step instanceof JanusGraphVertexStep) {
                    JanusGraphVertexStep vertexStep = (JanusGraphVertexStep)step;
                    if (vertices.isEmpty()) {
                        starts.forEachRemaining(start -> {
                            vertices.add((Admin<S>) start.split());
                        });
                        starts.add(vertices.iterator());
                        assert vertices.size() > 0;
                    }
                    vertexStep.initializeMultiQuery(vertices);
                }
            }
        }
    }
}

