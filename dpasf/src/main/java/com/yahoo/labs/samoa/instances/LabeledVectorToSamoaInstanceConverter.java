/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */

package com.yahoo.labs.samoa.instances;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a org.apache.flink.ml.common.LabeledVector Instance to a Samoa Instance
 *
 * @author Alejandro Alcalde <algui91@gmail.com>
 */
public class LabeledVectorToSamoaInstanceConverter implements Serializable {

    protected Instances samoaInstanceInformation;

    /**
     * Samoa instance from weka instance.
     *
     * @param inst the inst
     * @return the instance
     */
    public Instance samoaInstance(org.apache.flink.ml.common.LabeledVector inst) {
        Instance samoaInstance;

        double[] values = new double[inst.vector().size() + 1];
        for (int i = 1; i < inst.vector().size(); i++) {
            values[i] = inst.vector().apply(i);
        }
        values[0] = inst.label();
        samoaInstance = new DenseInstance(1, values);

        if (this.samoaInstanceInformation == null) {
            this.samoaInstanceInformation = this.samoaInstancesInformation(inst);
        }
        samoaInstance.setDataset(samoaInstanceInformation);
        samoaInstance.setClassValue(inst.label());
        return samoaInstance;
    }

    /**
     * Samoa instances from weka instances.
     *
     * @param instances the instances
     * @return the instances
     */
    public Instances samoaInstances(org.apache.flink.ml.common.LabeledVector instances) {
        Instances samoaInstances = samoaInstancesInformation(instances);
        //We assume that we have only one samoaInstanceInformation for WekaToSamoaInstanceConverter
        this.samoaInstanceInformation = samoaInstances;
//        for (int i = 0; i < instances.numInstances(); i++) {
//            samoaInstances.add(samoaInstance(instances.instance(i)));
//        }
        return samoaInstances;
    }

    /**
     * Samoa instances information.
     *
     * @param instances the instances
     * @return the instances
     */
    public Instances samoaInstancesInformation(org.apache.flink.ml.common.LabeledVector instances) {
        Instances samoaInstances;
        List<Attribute> attInfo = new ArrayList<>();
        for (int i = 0; i < instances.vector().size(); i++) {
            attInfo.add(samoaAttribute());
        }
        samoaInstances = new Instances("", attInfo, 0);
        samoaInstances.setClassIndex(0);
        return samoaInstances;
    }


    /**
     * Get Samoa attribute from a weka attribute.
     *
     * @return the attribute
     */
    protected Attribute samoaAttribute() {
        Attribute samoaAttribute;

        samoaAttribute = new Attribute();
        return samoaAttribute;
    }

}
