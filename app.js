const express = require('express');
const mysql = require('mysql2/promise');
const multer = require('multer');
const csvParser = require('csv-parser');
const fs = require('fs');
const app = express();

// MySQL connection details
const dbConfig = {
    host: 'srv1391.hstgr.io',
    user: 'u858543158_arpita',
    password: '2n:O!5:V',
    database: 'u858543158_arpitaDb'
};

// Middleware to handle file uploads
const upload = multer({ dest: 'uploads/' });

// Job functions mapping
const jobFunctions = {
    'Finance': [
        "Accounting", "Finance", "Financial Planning", "Financial Analysis", "Financial Reporting", 
        "Financial Strategy", "Financial Systems", "Internal Audit", "Internal Control", 
        "Investor Relations", "Mergers", "Acquisitions", "Real Estate Finance", "Financial Risk", 
        "Shared Services", "Sourcing", "Procurement", "Tax", "Treasury"
    ],
    'Product': [
        "Product Development", "Product Management"
    ],
    'Engineering & Technical': [
        "Artificial Intelligence", "Engineering", "Bioengineering", "Machine Learning", 
        "Biometrics", "Business Intelligence", "Chemical Engineering", "Cloud", "Mobility", 
        "Data Science", "DevOps", "Digital Transformation", "Innovation", "Engineering & Technical", 
        "Industrial Engineering", "Mechanic", "Mobile Development", "Project Management", 
        "Emerging Technology", "Agile Coach", "Scrum Master", "Software Development", 
        "Support", "Technical Services", "Technician", "Technology Operations", "Test", 
        "Quality Assurance", "UI", "UX", "Web Development"
    ],
    'Education': [
        "Teacher", "Principal", "Superintendent", "Professor", "Education"
    ],
    'Human Resources': [
        "Compensation", "Benefits", "Culture", "Diversity", "Labor Relations", 
        "Health & Safety", "Human Resource Information System", "Human Resources", 
        "HR Business Partner", "Learning & Development", "Organizational Development", 
        "Recruiting", "Talent Acquisition", "Talent Management", "Talent", 
        "Workforce Management", "People Operations"
    ],
    'Information Technology': [
        "Application Development", "Business Service Management", "Web App", "Data Center", 
        "Data Warehouse", "Database Administration", "eCommerce Development", 
        "Enterprise Architecture", "Help Desk", "Desktop Services", "ERP Systems", 
        "Information Security", "Information Technology", "Infrastructure", "IT Asset Management", 
        "IT Audit", "IT Compliance", "IT Operations", "IT Procurement", "IT Strategy", 
        "IT Training", "Networking", "Project & Program Management", "Quality Assurance", 
        "Store Systems", "Retail Systems", "Servers", "Storage", "Disaster Recovery", 
        "Telecommunications", "Virtualization", "Information technology", " IT "
    ],
    'Legal': [
        "Acquisitions", "Compliance", "Contracts", "Corporate Secretary", "eDiscovery", 
        "Ethics", "Governance", "Governmental Affairs", "Regulatory Law", "Intellectual Property", 
        "Patent", "Intellectual", "Labor & Employment", "Law", "Lawyer", "Attorney", 
        "Legal", "Legal Counsel", "Legal Operations", "Litigation", "Privacy"
    ],
    'Marketing': [
        "Advertising", "Brand Management", "Brand", "Advert", "Content Marketing", 
        "Customer Experience", "Customer Marketing", "Demand Generation", "Digital Marketing", 
        "eCommerce Marketing", "Event Marketing", "Field Marketing", "Lead Generation", 
        "Marketing", "Marketing Analytics", "Insights", "Marketing Communications", 
        "Marketing Operations", "Product Marketing", "Public Relations", "Search Engine Optimization", 
        "Pay Per Click", "Search Engine", "Social Media Marketing", "Strategic Communications", 
        "Communications", "Technical Marketing"
    ],
    'Medical & Health': [
        "Anesthesiology", "Chiropractics", "Clinical Systems", "Dentistry", "Dermatology", 
        "Physicians", "Doctors", "Epidemiology", "First Responder", "Infectious Disease", 
        "Medical Administration", "Medical Education", "Medical", "Medical Research", "Medicine", 
        "Neurology", "Nursing", "Nutrition", "Dietetics", "Obstetrics", "Gynecology", 
        "Oncology", "Opthalmology", "Optometry", "Orthopedics", "Pathology", "Pediatrics", 
        "Pharmacy", "Physical Therapy", "Psychiatry", "Psychology", "Public Health", 
        "Radiology", "Social Work"
    ],
    'Operations': [
        "Call Center", "Construction", "Corporate Strategy", "Customer Service", 
        "Client Services", "Customer Care", "Customer Support", "Enterprise Resource Planning", 
        "Facilities Management", "Leasing", "Logistics", "Office Operations", "Operations", 
        "Physical Security", "Project Development", "Quality Management", "Real Estate", 
        "Safety", "Operation", "Store Operations", "Supply Chain"
    ],
    'Sales': [
        "Account Management", "Outside Sales", "Field Sales", "Business Development", 
        "Channel Sales", "Customer Development", "Customer Retention", "Customer Success", 
        "Inside Sales", "Partnerships", "Revenue Operations", "Sales", "Sales Enablement", 
        "Sales Engineering", "Sales Operations", "Sales Training"
    ]
};

const countryAbbreviations = {
    'US': 'United States',
    'UK': 'United Kingdom',
    'USA': 'United States',
    'U.S.': 'United States',
    'CAN': 'Canada',
    'AU': 'Australia',
    'DE': 'Germany',
    'FR': 'France',
    'JP': 'Japan',
    'CN': 'China',
    'IN': 'India',
    'BR': 'Brazil',
    'ZA': 'South Africa',
    'MX': 'Mexico',
    'IT': 'Italy',
    'ES': 'Spain',
    'NL': 'Netherlands',
    'SE': 'Sweden',
    'NO': 'Norway',
    'DK': 'Denmark',
    'FI': 'Finland',
    'RU': 'Russia',
    'AR': 'Argentina',
    'CL': 'Chile',
    'CO': 'Colombia',
    'PE': 'Peru',
    'VE': 'Venezuela',
    'AU': 'Australia',
    'NZ': 'New Zealand',
    'SG': 'Singapore',
    'KR': 'South Korea',
    'MY': 'Malaysia',
    'TH': 'Thailand',
    'PH': 'Philippines',
    'ID': 'Indonesia',
    'TR': 'Turkey',
    'IL': 'Israel',
    'ZA': 'South Africa',
    'EG': 'Egypt',
    'NG': 'Nigeria',
    'KE': 'Kenya',
    'GH': 'Ghana',
    'MA': 'Morocco',
    'SA': 'Saudi Arabia',
    'AE': 'United Arab Emirates',
    'QA': 'Qatar',
    'KW': 'Kuwait',
    'OM': 'Oman',
    'JO': 'Jordan',
    'LB': 'Lebanon',
    'SY': 'Syria',
    'IQ': 'Iraq',
    'IR': 'Iran',
    'PK': 'Pakistan',
    'BD': 'Bangladesh',
    'LK': 'Sri Lanka',
    'NP': 'Nepal',
    'MV': 'Maldives',
    'PK': 'Pakistan',
    'UA': 'Ukraine',
    'BY': 'Belarus',
    'PL': 'Poland',
    'CZ': 'Czech Republic',
    'HU': 'Hungary',
    'RO': 'Romania',
    'BG': 'Bulgaria',
    'SK': 'Slovakia',
    'SI': 'Slovenia',
    'HR': 'Croatia',
    'RS': 'Serbia',
    'ME': 'Montenegro',
    'MD': 'Moldova',
    'GE': 'Georgia',
    'AM': 'Armenia',
    'AZ': 'Azerbaijan',
    'KZ': 'Kazakhstan',
    'UZ': 'Uzbekistan',
    'TJ': 'Tajikistan',
    'TM': 'Turkmenistan',
    'BG': 'Bulgaria',
    'LT': 'Lithuania',
    'LV': 'Latvia',
    'EE': 'Estonia',
    'IE': 'Ireland',
    'MT': 'Malta',
    'LU': 'Luxembourg',
    'MC': 'Monaco',
    'AD': 'Andorra',
    'SM': 'San Marino',
    'VA': 'Vatican City',
    'LI': 'Liechtenstein',
    'IS': 'Iceland',
    'MT': 'Malta',
    'CY': 'Cyprus',
    'RS': 'Serbia',
    'XK': 'Kosovo',
    'ME': 'Montenegro',
    'BW': 'Botswana',
    'ZW': 'Zimbabwe',
    'UG': 'Uganda',
    'TZ': 'Tanzania',
    'RW': 'Rwanda',
    'ET': 'Ethiopia',
    'SN': 'Senegal',
    'CI': 'Ivory Coast',
    'ML': 'Mali',
    'BF': 'Burkina Faso',
    'NE': 'Niger',
    'TD': 'Chad',
    'CM': 'Cameroon',
    'GA': 'Gabon',
    'CD': 'Democratic Republic of the Congo',
    'CG': 'Republic of the Congo'
};


const countriesList = [
    'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 
    'Antigua and Barbuda', 'Argentina', 'Armenia', 'Australia', 
    'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 
    'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 
    'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 
    'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 
    'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Central African Republic', 
    'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 
    'Congo, Democratic Republic of the', 'Congo, Republic of the', 
    'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 
    'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 
    'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 
    'Eswatini', 'Ethiopia', 'Fiji', 'Finland', 'France', 
    'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana', 
    'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau', 
    'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 
    'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 
    'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 
    'Kazakhstan', 'Kenya', 'Kiribati', 'Korea, North', 'Korea, South', 
    'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 
    'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 
    'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 
    'Maldives', 'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 
    'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco', 
    'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 
    'Namibia', 'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 
    'Nicaragua', 'Niger', 'Nigeria', 'North Macedonia', 'Norway', 
    'Oman', 'Pakistan', 'Palau', 'Palestine', 'Panama', 
    'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 
    'Portugal', 'Qatar', 'Romania', 'Russia', 'Rwanda', 
    'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Vincent and the Grenadines', 
    'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 
    'Senegal', 'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 
    'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 
    'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname', 
    'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan', 
    'Tanzania', 'Thailand', 'Timor-Leste', 'Turkey', 'Turkmenistan', 
    'Tuvalu', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 
    'United States', 'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican City', 
    'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 'Zimbabwe'
];

const natural = require('natural');
const { StringEncoder } = require('string_decoder'); // Ensure you have `natural` installed for fuzzy matching

const correctCountryName = (input) => {
    const normalizedInput = input.toUpperCase().trim();

    // Check if the input matches an abbreviation
    if (countryAbbreviations[normalizedInput]) {
        return countryAbbreviations[normalizedInput];
    }

    // Check for fuzzy matches in full country names
    let bestMatch = 'Unknown';
    let highestScore = 0;

    for (const country of countriesList) {
        const score = natural.JaroWinklerDistance(normalizedInput, country.toUpperCase());
        if (score > highestScore) {
            highestScore = score;
            bestMatch = country;
        }
    }

    return highestScore > 0.8 ? bestMatch : 'Unknown'; // Adjust the threshold as needed
};

// Generate a unique PID
const generateUniquePid = async (conn) => {
    while (true) {
        const pid = Date.now().toString(36) + Math.random().toString(36).substr(2);
        const [rows] = await conn.query("SELECT * FROM people WHERE pid = ?", [pid]);
        if (rows.length === 0) return pid;
    }
};


// API to process CSV upload
app.post('/upload-bclfile', upload.single('file'), async (req, res) => {
    const filePath = req.file.path;
    const conn = await mysql.createConnection(dbConfig);

    let rowsToUpdate = 0;
    let rowsToInsert = 0;
    let updatedRows = [];
    let insertedRows = [];
    let distinctEmails = new Set();
    let prospectLinkCounts = {};
    let pidCounts = {};
    let duplicateProspectLinks = 0;
    let duplicatePIDs = 0;

    fs.createReadStream(filePath)
        .pipe(csvParser())
        .on('data', async (row) => {
            // Clean the row data
        let data = cleanData(row);
        console.log(data);
        // Further processing or saving the cleanedRow

            // Set job function based on job title
            data.job_function = 'Any';
            for (let [functionName, titles] of Object.entries(jobFunctions)) {
                if (titles.some(title => data.job_title?.toLowerCase().includes(title.toLowerCase()))) {
                    data.job_function = functionName;
                    break;
                }
            }

            // Set job level based on job title
            data.job_level = setJobLevel(data.job_title);

            // Set employee size category
            data.employee_size = setEmployeeSizeCategory(data.employee_size);

            distinctEmails.add(data.email_validation);

            // Check if record exists
            const [existingRows] = await conn.query("SELECT * FROM people WHERE prospect_link = ?", [data.prospect_link]);

            if (existingRows.length > 0) {
                // Existing record found
                let existingData = existingRows[0];

                // Ensure PID is set and unique
                if (!existingData.pid) {
                    existingData.pid = await generateUniquePid(conn);
                    await conn.query("UPDATE people SET pid = ? WHERE prospect_link = ?", [existingData.pid, data.prospect_link]);
                }

                pidCounts[existingData.pid] = (pidCounts[existingData.pid] || 0) + 1;

                let updateNeeded = Object.keys(data).some(key => key !== 'prospect_link' && key !== 'pid' && data[key] !== existingData[key]);

                if (updateNeeded) {
                    await conn.query(`
                        UPDATE people SET first_name=?, last_name=?, email_address=?, company_name=?, company_domain=?, 
                        job_title=?, job_function=?, job_level=?, company_address=?, city=?, State=?, zip_code=?, country=?, 
                        telephone_number=?, employee_size=?, industry=?, revenue=?, sic=?, naic=?, company_link =?, email_validation=?, pid=? 
                        WHERE prospect_link=?`, [
                        data.first_name, data.last_name, data.email_address, data.company_name, data.company_domain,
                        data.job_title, data.job_function, data.job_level, data.company_address, data.city, data.State,
                        data.zip_code, data.country, data.telephone_number, data.employee_size, data.industry, data.revenue,
                        data.sic, data.naic, data.company_link, data.email_validation, existingData.pid, data.prospect_link
                    ]);

                    rowsToUpdate++;
                    updatedRows.push(data.prospect_link);
                }
            } else {
                // New record
                data.pid = await generateUniquePid(conn);

                pidCounts[data.pid] = (pidCounts[data.pid] || 0) + 1;

                await conn.query(`
                    INSERT INTO people (first_name, last_name, email_address, company_name, company_domain, job_title, job_function, 
                    job_level, company_address, city, State, zip_code, country, telephone_number, employee_size, industry, revenue, 
                    sic, naic, company_link, email_validation, pid, prospect_link) VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
                    data.first_name, data.last_name, data.email_address, data.company_name, data.company_domain, data.job_title, data.job_function,
                    data.job_level, data.company_address, data.city, data.State, data.zip_code, data.country, data.telephone_number, data.employee_size,
                    data.industry, data.revenue, data.sic, data.naic, data.company_link, data.email_validation, data.pid, data.prospect_link
                ]);

                rowsToInsert++;
                insertedRows.push(data.prospect_link);
            }

            prospectLinkCounts[data.prospect_link] = (prospectLinkCounts[data.prospect_link] || 0) + 1;
        })
        .on('end', async () => {
            const [totalCountRow] = await conn.query("SELECT COUNT(*) as total_count FROM people");
            const totalDataCount = totalCountRow[0].total_count;

            const [duplicatePCountRow] = await conn.query(`
                SELECT COUNT(*) as duplicate_p_count 
                FROM (SELECT prospect_link FROM people GROUP BY prospect_link HAVING COUNT(*) > 1) as duplicates`);
            const duplicate_P_Count = duplicatePCountRow[0].duplicate_p_count;

            const [duplicatePidCountRow] = await conn.query(`
                SELECT COUNT(*) as duplicate_pid_count 
                FROM (SELECT pid FROM people GROUP BY pid HAVING COUNT(*) > 1) as duplicates`);
            const duplicate_pid_count = duplicatePidCountRow[0].duplicate_pid_count;

            conn.end();

            res.json({
                total_data_count: totalDataCount,
                duplicate_p_count: duplicate_P_Count,
                new_rows_inserted: rowsToInsert,
                existing_rows_updated: rowsToUpdate,
                distinct_email_validations: Array.from(distinctEmails),
                duplicate_pid_counts: duplicate_pid_count
            });

            fs.unlinkSync(filePath); // Remove the uploaded file after processing
        })
        .on('error', (err) => {
            res.status(500).json({ error: err.message });
        });
});

function cleanData(data) {
    for (let key in data) {
        // Trim the key and value
        let trimmedKey = key.trim();
        let value = data[key]?.toString().trim(); // Convert to string and trim


        if (value !== undefined && value !== null) {
            // Skip cleaning for specific fields
            if (trimmedKey === 'company_link' || trimmedKey === 'company_address') {
                // Preserve the value as is
                data[trimmedKey] = value
                     .replace(/[^\x20-\x7E]/g, '') // Remove non-printable characters
                    .trim() // Remove leading and trailing spaces
                    .replace(/\s{2,}/g, ' '); // Replace multiple spaces with a single space

            } else {
                // Clean other fields
                data[trimmedKey] = value
                    .replace(/[!#$*,<>'{}[\]~?`]/g, '') // Remove specific special characters
                    .replace(/[^\x20-\x7E]/g, '') // Remove non-printable characters
                    .trim() // Remove leading and trailing spaces
                    .replace(/\s{2,}/g, ' '); // Replace multiple spaces with a single space

                // Convert empty strings or "-" to NULL
                if (data[trimmedKey] === '' || data[trimmedKey] === '-') {
                    data[trimmedKey] = null;
                }
            }
        } else {
            // Ensure that null or undefined values are not processed
            data[trimmedKey] = null;
        }
    }
      // Correct the country name
    if (data.country) {
        data.country = correctCountryName(data.country);
    }

     // Remove specific unwanted characters from 'State' field
 
   // Clean employee_size field
    // Clean and categorize employee_size
    // if (data.employee_size) {
    //     data.employee_size = setEmployeeSizeCategory(data.employee_size); // Apply categorization
    // }

      // Map email_validation field
      if (data.email_validation) {
        switch (data.email_validation.trim().toLowerCase()) {
            case 'cleared':
                data.email_validation = 'Deliverable';
                break;
            case 'ebb':
                data.email_validation = 'Undeliverable';
                break;
            case 'pending':
                data.email_validation = 'Unknown';
                break;
            default:
                data.email_validation = 'Unknown'; // Default case for unexpected values
        }
    }

    return data;
}

function setJobLevel(jobTitle) {
    if (!jobTitle) return 'Individual Contributor';

    const title = jobTitle.toLowerCase();
    if (/(manager|head|team lead)/i.test(title)) return 'Manager';
    if (/(director)/i.test(title)) return 'Director';
    if (/(vice president|vp|vice|VP|VICE|EVP|evp)/i.test(title)) return 'VP Level';
    if (/(chief|cio|cto|cmo|coo|ciso|cco|cdo|cfo|ceo|chro|cao|cmio|cpo|cro|cso)/i.test(title)) return 'C Level';
    return 'Individual Contributor';
}

function setEmployeeSizeCategory(size) {
    if (!size) return 'Unknown';

    // Clean the employee_size field
    size = size.replace(/[^0-9\-+]/g, ''); // Remove non-numeric characters except '-' and '+'
    console.log('Cleaned employee_size:', size); // Log the cleaned value for debugging

    // Determine the employee size category
    if (/(\d+)-(\d+)/.test(size)) {
        const [low, high] = size.split('-').map(Number);
        if (low >= 10001) return '10000+';
        if (low >= 5001) return '5001-10000';
        if (low >= 1001) return '1001-5000';
        if (low >= 501) return '501-1000';
        if (low >= 201) return '201-500';
        if (low >= 51) return '51-200';
        if (low >= 11) return '11-50';
        return '1-10';
    } else if (/(\d+)\+/.test(size)) {
        const num = parseInt(size, 10);
        if (num >= 10001) return '10000+';
        if (num >= 5001) return '5001-10000';
        if (num >= 1001) return '1001-5000';
        if (num >= 501) return '501-1000';
        if (num >= 201) return '201-500';
        if (num >= 51) return '51-200';
        if (num >= 11) return '11-50';
        return '1-10';
    }
    return 'Unknown';
}


app.listen(3000, () => {
    console.log('Server is running on port 3000');
});




