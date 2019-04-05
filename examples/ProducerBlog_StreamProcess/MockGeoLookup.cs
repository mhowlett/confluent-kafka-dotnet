using System;
using System.Threading.Tasks;


namespace ProducerBlog_StatelessProcessing
{
    public static class MockGeoLookup
    {
        private static Random r = new Random();

        private static string[] CountriesOrderedByPopulation => new [] { "China", "India", "United States", "Indonesia", "Brazil", "Pakistan", "Nigeria", "Bangladesh", "Russia", "Mexico", "Japan", "Ethiopia", "Philippines", "Egypt", "Vietnam", "DR Congo", "Turkey", "Iran", "Germany", "Thailand", "United Kingdom", "France", "Tanzania", "Italy", "South Africa", "Myanmar", "Kenya", "South Korea", "Colombia", "Spain", "Uganda", "Argentina", "Ukraine", "Algeria", "Sudan", "Iraq", "Poland", "Canada", "Afghanistan", "Morocco", "Saudi Arabia", "Peru", "Uzbekistan", "Venezuela", "Malaysia", "Angola", "Mozambique", "Ghana", "Nepal", "Yemen", "Madagascar", "North Korea", "Ivory Coast", "Cameroon", "Australia", "Taiwan", "Niger", "Sri Lanka", "Burkina Faso", "Malawi", "Mali", "Romania", "Kazakhstan", "Syria", "Chile", "Zambia", "Guatemala", "Zimbabwe", "Netherlands", "Ecuador", "Senegal", "Cambodia", "Chad", "Somalia", "Guinea", "South Sudan", "Rwanda", "Benin", "Tunisia", "Burundi", "Belgium", "Cuba", "Bolivia", "Haiti", "Greece", "Dominican Republic", "Czech Republic", "Portugal", "Jordan", "Sweden", "Azerbaijan", "United Arab Emirates", "Hungary", "Honduras", "Belarus", "Tajikistan", "Austria", "Serbia", "Switzerland", "Papua New Guinea", "Israel", "Togo", "Sierra Leone", "Hong Kong", "Laos", "Bulgaria", "Paraguay", "Libya", "El Salvador", "Nicaragua", "Kyrgyzstan", "Lebanon", "Turkmenistan", "Singapore", "Denmark", "Finland", "Republic of the Congo", "Slovakia", "Norway", "Eritrea", "Palestine", "Oman", "Costa Rica", "Liberia", "Ireland", "Central African Republic", "New Zealand", "Mauritania", "Kuwait", "Panama", "Croatia", "Moldova", "Georgia", "Puerto Rico", "Bosnia and Herzegovina", "Uruguay", "Mongolia", "Albania", "Armenia", "Jamaica", "Lithuania", "Qatar", "Namibia", "Botswana", "Lesotho", "Gambia", "Gabon", "Macedonia", "Slovenia", "Guinea-Bissau", "Latvia", "Bahrain", "Swaziland", "Trinidad and Tobago", "Equatorial Guinea", "Timor-Leste", "Estonia", "Mauritius", "Cyprus", "Djibouti", "Fiji", "Reunion", "Comoros", "Bhutan", "Guyana", "Macau", "Solomon Islands", "Montenegro", "Luxembourg", "Western Sahara", "Suriname", "Cape Verde", "Maldives", "Guadeloupe", "Brunei", "Malta", "Bahamas", "Belize", "Martinique", "Iceland", "French Guiana", "French Polynesia", "Vanuatu", "Barbados", "New Caledonia", "Mayotte", "Sao Tome and Principe", "Samoa", "Saint Lucia", "Guam", "Curacao", "Kiribati", "Saint Vincent and the Grenadines", "Tonga", "Grenada", "Micronesia", "Aruba", "United States Virgin Islands", "Antigua and Barbuda", "Seychelles", "Isle of Man", "Andorra", "Dominica", "Cayman Islands", "Bermuda", "Greenland", "Saint Kitts and Nevis", "American Samoa", "Northern Mariana Islands", "Marshall Islands", "Faroe Islands", "Sint Maarten", "Monaco", "Liechtenstein", "Turks and Caicos Islands", "Gibraltar", "San Marino", "British Virgin Islands", "Palau", "Cook Islands", "Anguilla", "Wallis and Futuna", "Tuvalu", "Nauru", "Saint Pierre and Miquelon", "Montserrat", "Falkland Islands", "Niue", "Tokelau", "Vatican City" };

        public async static Task<string> GetCountryFromIPAsync(string ip)
        {
            await Task.Delay(100);
            return CountriesOrderedByPopulation[r.Next(0, CountriesOrderedByPopulation.Length)];
        }
    }
}
