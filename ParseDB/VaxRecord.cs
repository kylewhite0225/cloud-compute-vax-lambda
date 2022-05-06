using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParseDB;

public class VaxRecord
{
    public String SideID
    {
        get;
        set;
    }

    public String Name
    {
        get;
        set;
    }

    public String ZipCode
    {
        get;
        set;
    }

    public String Date
    {
        get;
        set;
    }

    public int FirstShot
    {
        get;
        set;
    }

    public int SecondShot
    {
        get;
        set;
    }
}