package org.apache.flink.runtime.maqy;

import gurobi.*;

/**
 * This class is used to calculate the best proportion of location
 * <p>
 * min z
 * s.t.
 *   i >= 0
 *   i1 + i2 + ... + in =1
 *   Tu <= z  Td <= z
 *   Tu = (1 - r)*S'/U
 *   Td = r(S - S')/D
 * S is the total data size , S' is one of the location's data size
 */
public class LPUtil {
	public boolean getLocationProportion(LocationInfo[] locationInfos) throws GRBException {
		if (locationInfos == null || locationInfos.length == 0) {
			return false;
		}
		// 得到需要计算比例的location个数
		int n = locationInfos.length;

		GRBEnv env = new GRBEnv("solveLP.log");
		GRBModel model = new GRBModel(env);

		// Create variables
		GRBVar[] r = new GRBVar[n];
		for (int i = 0; i < r.length; i++) {
			r[i] = model.addVar(0.0, GRB.INFINITY, 0.0, GRB.CONTINUOUS, "r" + i);
		}
		GRBVar z = model.addVar(-GRB.INFINITY, GRB.INFINITY, 1.0, GRB.CONTINUOUS, "z");

		// Set objective: minimize z
		GRBLinExpr expr = new GRBLinExpr();
		expr.addTerm(1.0, z);
		model.setObjective(expr, GRB.MINIMIZE);

		int totalSize = 0;

		// Add constraint: r1 + r2 + ... + rn = 1
		expr = new GRBLinExpr();
		for (int i = 0; i < r.length; i++) {
			expr.addTerm(1.0, r[i]);
			// By the way, calculate the total data size
			totalSize = totalSize + locationInfos[i].getDataSize();
		}
		model.addConstr(expr, GRB.EQUAL, 1.0, "c0");

		// Add other Tu and Td constraint
		int num = 1;
		for (int i = 0; i < locationInfos.length; i++) {
			LocationInfo locationinfo = locationInfos[i];
			//这里用float还是int 待定，int速度可能会快些，但不知道精度如何
			float upLink = locationinfo.getUpLink();
			float downLink = locationinfo.getDownLink();
			float dataSize = locationinfo.getDataSize();

			// Add Tu constraint     -(Si / Ui) * ri - z <= -(Si / Ui)
			expr = new GRBLinExpr();
			expr.addTerm(-1 * (dataSize / upLink), r[i]);
			expr.addTerm(-1.0, z);
			model.addConstr(expr, GRB.LESS_EQUAL, -1 * (dataSize / upLink), "c" + num);
			++num;
			// Add Td constraint  [(S - Si) / Di] * ri - z <= 0
			expr = new GRBLinExpr();
			expr.addTerm((totalSize - dataSize) / downLink, r[i]);
			expr.addTerm(-1.0, z);
			model.addConstr(expr, GRB.LESS_EQUAL, 0.0, "c" + num);
			++num;
		}

		//Optimize model
		model.write("solveModel.lp");
		model.optimize();

		for (int i = 0; i < r.length; i++) {
			int proportion=(int)(100*r[i].get(GRB.DoubleAttr.X));
			//System.out.println(r[i].get(GRB.StringAttr.VarName) + " " + (int)(100*r[i].get(GRB.DoubleAttr.X)));
			locationInfos[i].setProportion(proportion);
		}

		System.out.println("Obj: " + model.get(GRB.DoubleAttr.ObjVal));

		// Dispose of model and environment
		model.dispose();
		env.dispose();
		return true;
	}
}

