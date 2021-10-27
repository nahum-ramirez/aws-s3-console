using System.IO;
using System.Linq;

namespace S3Console
{
    public static class Utils
    {
        public static bool IsPathFile(string path)
        {
            return (!new[] { "\\", "/" }.Any(a => path.EndsWith(a))
                && !string.IsNullOrWhiteSpace(Path.GetExtension(path)));
        }

        public static void CreateFileDirectory(string path)
        {            
            if (IsPathFile(path))
            {
                FileInfo file = new FileInfo(path);
                if (!file.Directory.Exists) file.Directory.Create();
            }
            else
            {
                if (!Directory.Exists(path)) Directory.CreateDirectory(path);
            }
        }
    }
}
