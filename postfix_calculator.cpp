//
// Postfix(Reverse Polish Notation) Calculator
// http://en.wikipedia.org/wiki/Reverse_Polish_Notation
//
// Author: Zhihui Chen
// Date: 2013-07-31
//

#include <iostream>
#include <sstream>
#include <set>
#include <map>
#include <stack>
#include <vector>
#include <cmath>
#include <algorithm>

using namespace std;

bool add(const vector<double>& vec, double& res)
{
	if (vec.size() < 2)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = vec[0] + vec[1];
	return true;
}

bool sub(const vector<double>& vec, double& res)
{
	if (vec.size() < 2)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = vec[0] - vec[1];
	return true;
}

bool mul(const vector<double>& vec, double& res)
{
	if (vec.size() < 2)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = vec[0] * vec[1];
	return true;
}

bool div(const vector<double>& vec, double& res)
{
	if (vec.size() < 2)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = vec[0] / vec[1];
	return true;
}

bool sqrt(const vector<double>& vec, double& res)
{
	if (vec.size() < 1)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = std::sqrt(vec[0]);
	return true;
}

bool factorial(const vector<double>& vec, double& res)
{
	if (vec.size() < 1)
	{
		cerr << "number of operands does not match." << endl;
		return false;
	}
	res = 1;
	for (int i = 1; i <= vec[0]; i++)
		res *= i;
	return true;
}


class Calculator
{
public:
	typedef bool (*OPFUNC)(const vector<double>& vec, double& res);

	Calculator() {}
	~Calculator() {}

	void Run()
	{
		cout << ">> ";
		string line;

		while (getline(cin, line) && line != "exit")
		{
			clear();
			istringstream iss(line);
			string item;
			success = true;

			while (iss >> item)
			{
				if ((op_it = op2cnts.find(item)) != op2cnts.end())
				{
					process_operator(item);
				}
				else
				{
					process_operand(item);
				}
			}
			if (success)
				cout << res << endl;
			cout << ">> ";
		}
	}

	void Register(const string& sign, OPFUNC func, int num)
	{
		op2funcs.insert(make_pair(sign, func));
		op2cnts.insert(make_pair(sign, num));
	}

	void DeRegister(const string& sign)
	{
		if ((op_it = op2cnts.find(sign)) != op2cnts.end())
		{
			op2funcs.erase(sign);
			op2cnts.erase(sign);
		}
	}

private:
	stack<double> oprds;
	double res;
	bool success;
	map<string, OPFUNC> op2funcs;
	map<string, int> op2cnts;
	map<string, int>::iterator op_it;

	void process_operator(const string& item)
	{
		if (oprds.size() < op2cnts[item])
		{
			error("Not enough operands");
		}
		else
		{
			vector<double> vec;
			for (int i = 0; i < op2cnts[item]; i++)
			{
				vec.push_back(oprds.top());
				oprds.pop();
			}
			reverse(vec.begin(), vec.end());

			if((*op2funcs[item])(vec,res))
			{
				oprds.push(res);
			}
			else
			{
				error("error: " + item);
			}
		}
	}

	void process_operand(const string& item)
	{
		stringstream ss(item);
		int opr;
		if ((ss >> opr).fail())
		{
			error("Fail to convert: " + item);
		}
		else
		{
			oprds.push(static_cast<double>(opr));
		}
	}

	void error(const string& msg = "")
	{
		cerr << msg << endl;
		clear();
		success = false;
	}

	void clear()
	{
		while (!oprds.empty())
			oprds.pop();
		res = 0;
	}
};

int main(int argc, char** argv)
{
	Calculator calc;
	
	calc.Register("+", &add, 2);
	calc.Register("-", &sub, 2);
	calc.Register("*", &mul, 2);
	calc.Register("/", &div, 2);
	calc.Register("sqrt", &sqrt, 1);
	calc.Register("!", &factorial, 1);
	
	calc.Run();

	return 0;
}