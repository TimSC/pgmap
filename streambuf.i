%inline %{
//Based on http://swig.10945.n7.nabble.com/Using-std-istream-from-Python-tp3733p3735.html
//Add this to your SWIG interface file
class CPyOutbuf : public std::streambuf
{
public:
	CPyOutbuf(PyObject* obj) {
		m_PyObj = obj;
		Py_INCREF(m_PyObj);
		m_Write = PyObject_GetAttrString(m_PyObj, "write");
	}
	~CPyOutbuf() {
		Py_XDECREF(m_Write);
		Py_XDECREF(m_PyObj);
	}
protected:
	int overflow (int c = EOF)
	{
		if(m_Write != nullptr)
		{
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"c", c);
			Py_XDECREF(ret);
		}
		return c;
	}
	std::streamsize xsputn(const char* s, std::streamsize count) {
		if(m_Write != nullptr)
		{
			#if PY_MAJOR_VERSION < 3
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"s#", s, int(count));
			#else
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"y#", s, int(count));
			#endif 
			Py_XDECREF(ret);
		}
		return count;
	}
	PyObject* m_PyObj;
	PyObject* m_Write;
};
%} 

