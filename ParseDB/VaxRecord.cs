using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParseDB;

public class VaxRecord
{
    public Site Site
    {
        get;
        set;
    }
    
    public Date Date
    {
        get;
        set;
    }

    public Vaccines[] Vaccines
    {
        get;
        set;
    }

}